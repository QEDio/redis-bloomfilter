require "spec_helper"
require "set"

def test_error_rate(bf,elems)
  visited = Set.new
  error = 0

  elems.times do |i|
    a = rand(elems)
    error += 1 if bf.include?(a) != visited.include?(a)
    visited << a
    bf.insert a
  end

  error.to_f / elems
end

def factory options, driver
  options[:driver] = driver
  Redis::Bloomfilter.new options
end

describe Redis::Bloomfilter do

  it 'should return the right version' do
    Redis::Bloomfilter.version.should eq "redis-bloomfilter version #{Redis::Bloomfilter::VERSION}"
  end

  it 'should check for the initialize options' do
    expect { Redis::Bloomfilter.new }.to raise_error(ArgumentError)
    expect { Redis::Bloomfilter.new :size => 123 }.to raise_error(ArgumentError)
    expect { Redis::Bloomfilter.new :error_rate => 0.01 }.to raise_error(ArgumentError)
    expect { Redis::Bloomfilter.new :size => 123,:error_rate => 0.01, :driver => 'bibu' }.to raise_error(NameError)
  end

  it 'should choose the right driver based on the Redis version' do
    
    redis_mock = flexmock("redis")
    redis_mock.should_receive(:info).and_return({'redis_version' => '2.6.0'})
    redis_mock.should_receive(:script).and_return([true, true])
    redis_mock_2_5 = flexmock("redis_2_5")
    redis_mock_2_5.should_receive(:info).and_return({'redis_version' => '2.5.0'})

    bf = factory({:size => 1000, :error_rate => 0.01, :key_name => 'ossom', :redis => redis_mock}, nil)
    bf.driver.should be_kind_of(Redis::BloomfilterDriver::Lua)

    bf = factory({:size => 1000, :error_rate => 0.01, :key_name => 'ossom', :redis => redis_mock_2_5}, nil)
    bf.driver.should be_kind_of(Redis::BloomfilterDriver::Ruby)
  end

  it 'should create a Redis::Bloomfilter object' do
    bf = factory({:size => 1000, :error_rate => 0.01, :key_name => 'ossom'}, 'ruby')
    bf.should be
    bf.options[:size].should eq 1000
    bf.options[:bits].should eq 9585
    bf.options[:hashes].should eq 6
    bf.options[:key_name].should eq 'ossom'
    bf.clear
  end

  %w(ruby lua ruby-test).each do |driver|
  # %w(ruby ruby-test).each do |driver|
    context "testing #{driver}" do
      let(:data_arr) {['abc', 'xyz', '123']}
      let(:data) {'hij'}
      let(:bf){factory({:size => 1000, :error_rate => 0.01, :key_name => '__test_bf'},driver)}

      before do
        bf.clear
      end

      context '#insert' do
        it 'works with a single element to insert' do
          bf.include?(data).should be false
          bf.insert data
          bf.include?(data).should be true
          bf.clear
          bf.include?(data).should be false
        end

        it 'works with an array of elements to insert' do
          data_arr.each do |el|
            bf.include?(el).should be false
          end

          bf.insert data_arr
          data_arr.each do |el|
            bf.include?(el).should be true
          end

          bf.clear
          data_arr.each do |el|
            bf.include?(el).should be false
          end
        end
      end

      context '#remove' do
        it 'removes a single element from the list' do
          bf.insert(data)
          bf.include?(data).should be true

          bf.remove data
          bf.include?(data).should be false
        end

        it 'removes an array of elements from the list' do
          # in general this is also tested in the #include? spec
          # but better to have to fix two tests than not seeing an error
          bf.insert(data_arr)
          bf.include?(data_arr).should eq(data_arr)

          bf.remove(data_arr)
          bf.include?(data_arr).should eq([])
        end
      end

      context '#include?' do
        it 'works with a single element' do
          bf.insert(data)
          bf.include?(data).should be true
        end

        it 'returns an array with all elements that are include?(el) == true if el is a single element (1)' do
          bf.insert(data)
          known_elements = bf.include?([data, '123', '456'])
          known_elements.length.should eq(1)
          known_elements.should eq([data])
        end

        it 'returns an array with all elements that are include?(el) == true if el is a single element (2)' do
          bf.insert(data_arr)
          # we might need to sort the returned elements
          known_elements = bf.include?(data_arr + ['123', '456'])
          known_elements.should eq(data_arr)
        end
      end

      context '#error_rate' do
        it 'should honor the error rate' do
          e = test_error_rate bf, 180
          e.should be < bf.options[:error_rate]
          bf.clear
        end
      end
    end
  end

  context 'should be a scalable bloom filter for' do
    %w(ruby lua ruby-test).each do |driver|
      context "#{driver}" do
        let(:bf){factory({:size => 5, :error_rate => 0.01, :key_name => '__test_bf'},driver)}

        before do
          bf.clear
        end

        it "scales" do
          # this doesn't work with ruby/ruby-test since those are not scalable
          e = test_error_rate(bf, 1000)
          e.should be < bf.options[:error_rate]
          bf.clear
        end
      end
    end
  end
end