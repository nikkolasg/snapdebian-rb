require 'ostruct'
require 'thread'

require_relative'log'

Struct.new("Snapshot",:time,:packages)

class Formatter

    ## TODO correct sha256_orig
    @checksum_field = "Checksums-Sha256".downcase.to_sym
    @source_fields = [:package,:version,:hash_source]
    @binary_fields = [:package,:version,:hash_binary]
    class << self
        attr_accessor :source_fields 
        attr_accessor :binary_fields
        attr_accessor :checksum_field
    end

    require 'xz'
    require 'debian_control_parser'
    require 'open-uri'

    def initialize packages = nil
        @packages = packages
    end

    ## format takes a [time] => [binarylink,sourceLink]
    # and yields each snapshots once formatted
    def format links
        links.each do |k,v|
            yield process_snapshot k,v[:source],v[:binary]
        end
    end
    private
    ## create_snapshot takes links to source.xz file & binary.xz file. It
    #decompress them, analyzes them and return an snapshot struct
    def process_snapshot time,source,binary
        $logger.info "Processing snapshot @ #{time}"
        packages = Hash.new{|h,k| h[k] = {}}
        nb_source = 0
        download_process source do |hash|
            formatted = format_source hash
            packages[formatted[:package]].merge! formatted
            nb_source += 1
        end
        $logger.debug "Found #{nb_source} sources"

        nb_binaries = 0
        nb_mismatch = 0
        download_process binary do |hash|
            formatted = format_binary hash
            p = packages[formatted[:package]] 
            if p[:version] != formatted[:version]
                nb_mismatch += 1
                next
            end
            p[:hash_binary] = formatted[:hash_binary]
            nb_binaries += 1
        end
        $logger.debug "Found #{nb_binaries} binaries and #{nb_mismatch} mismatches"
        $logger.debug "Example #{packages[packages.keys.first]}"
        return Struct::Snapshot.new(time,packages)
    end

    def format_source hash
        ## Multiline ...
        hash[Formatter.checksum_field].split("\n").each do |line|
            ## search for the ***.orig.tar.xz file sha256 in hexadecimal
            #followed by anything with "orig.tar" inside
            next false unless line =~ /(\w{64}).*\orig\.tar.*/
            hash[:hash_source] = $1 
        end
        hash.delete(Formatter.checksum_field)
        ## take what we need
        slice hash,*Formatter.source_fields
    end

    def format_binary hash
        hash[:hash_binary] = hash[:sha256] 
        hash.delete(:sha256)
        slice hash,*Formatter.binary_fields
    end

    ## it will download and decompress the file at the same time 
    # returning an stream over the decompressed file
    def download_decompress link
        open(link) do |f|
            reader = XZ::StreamReader.new f
            yield reader
            reader.finish ## ?? close raise the deprecation warning ><
        end
    end

    ## download_process takes a link and a block. It downloads & uncompress the
    #file with download_decompress and iterates over each paragraph and each
    #value. For each paragraph it creates a OpenStruct and for each key/value #
    #it yields the block. The blocks analyzes the key/value and return the value
    #if it wants to add it to the Open struct or nil otherwise, Each struct is
    #appended to the collection that is returned at the end.
    # If @packages is not nil,then it must be a list of package names to
    # filter from the debian files, to only select informations for theses
    # packages.
    # # TODO re-write comments
    def download_process link
        download_decompress link  do |data|
            parser = DebianControlParser.new data
            parser.paragraphs do |p|
                obj = {}
                p.fields do |name,value|
                    n = name.downcase.strip.to_sym
                    obj[n] = value
                end
                if !@packages.empty? && !@packages.include?(obj[:package])
                    next
                end
                yield obj
            end
        end
    end

    def slice(hash, *keys)
        Hash[ [keys, hash.values_at(*keys)].transpose]
    end
end

## this class is responsible to handle all snapshot given to it
# It write the information to a csv files and generate the folders + files
# corresponding
class Processor

    require 'toml'

    @csv = "snapshots.csv"

    Struct.new("Policy",:package,:version,:threshold,:hash_binary,:hash_source)

    class << self
        attr_accessor :csv
    end

    def initialize folder,csv = nil
        @folder = folder
        Dir.mkdir(File.join(Dir.pwd,@folder)) unless File.directory?(@folder)
        @csv = csv || Processor.csv
        @file = File.open(@csv,"w+") 
        @file.write "time, name, version\n"
        @queue = Queue.new
        @mutex = Mutex.new
        @thread = nil
    end

    ## go creates a processor that runs a thread that receives snapshots
    ## it yield the processor to call `append` on it each time a new snapshot
    #arrives
    def self.go folder,&block
        proc = Processor.new folder
        proc.run &block
    end



    def append snapshot
        @mutex.synchronize do
            snapshot.packages.each do |name,hash|
                @file.write [snaphost.time,name,hash[:version]].join(",")
                @file.write "\n"
            end
        end
    end

    def run 
        @thread = Thread.new do 
            while 
                snapshot = @queue.pop
                return unless snapshot 
                append snapshot
                create_policy snapshot
            end
        end

        yield self 

        @queue << nil
        @mutex.synchronize do
            @file.close
        end
    end

    private 
    def create_policy snapshot
        snapshot.packages.each do |k,v| 
            package_folder = File.join(Dir.pwd,@folder,k)
            Dir.mkdir(package_folder) unless File.directory?(package_folder)
            policy = "policy_" + v[:version] + ".toml"
            File.open(policy,"w+") {|f| f.write TOML.dump(v) } 
        end
    end

    ## push adds  snapshot to the queue so the thread can process it.
    def push  snapshot
        @queue << snapshot
    end

end
