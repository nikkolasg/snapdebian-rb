require 'ostruct'
require 'thread'

require_relative'log'

class Snapshot

    attr_accessor :packages

    def initialize time,packages
        @time = Time.parse time
        @packages = packages
        @str_time = @time.strftime("%Y%m%d%H%M%S")
    end

    def <=> snap
        self.time <=> snap.time
    end

    def time_format
        @str_time
    end

end

class Formatter
    ## TODO correct sha256_orig
    @csv = "snapshots.csv"
    @checksum_field = "Checksums-Sha256".downcase.to_sym
    @source_fields = [:package,:version,:hash_source]
    @binary_fields = [:package,:version,:hash_binary]
    class << self
        attr_accessor :source_fields 
        attr_accessor :binary_fields
        attr_accessor :checksum_field
        attr_accessor :csv
    end

    require 'xz'
    require 'debian_control_parser'
    require 'open-uri'
    require_relative 'ruby_util'
    require 'etc'

    def initialize folder,packages = nil,csv = nil
        @folder = folder
        Dir.mkdir(File.join(Dir.pwd,@folder)) unless File.directory?(@folder)
        @csv = File.join(@folder,Formatter.csv)
        @cache = File.join(@folder,"cache")
        Dir.mkdir @cache unless File.directory? @cache
        @packages = packages
    end

    ## format takes a [time] => [binarylink,sourceLink]
    # and yields each snapshots once formatted
    def format links
        @file = File.open(@csv,"w") 
        @file.write "time, name, version, hash_source, hash_binary\n"
        @file.flush
        RubyUtil::partition_by_size(links.keys,1) do |times|
            times.each do |time|
                v = links[time]
                snapshot = process_snapshot time,v[:source],v[:binary]
                append snapshot
            end
        end
        @file.close
        $logger.info "Insert #{`cat #{@csv} | wc -l`.strip} lines in the #{@csv}"
    end

    private

    def append snapshot
        snapshot.packages.each do |p,info|
            str = [snapshot.time_format,p,info[:version],info[:hash_source],info[:hash_binary]].join(",")
            if info[:version].nil? || info[:version].empty? || info[:hash_source].nil? || info[:hash_source].empty?
                puts "EMPTY #{p} => #{info}"
                sleep 1
                next
            end
            @file.write str + "\n"
        end
    end
    ## create_snapshot takes links to source.xz file & binary.xz file. It
    #decompress them, analyzes them and return an snapshot struct
    def process_snapshot time,source,binary
        $logger.info "Processing snapshot @ #{time}"
        packages = {}
        nb_source = 0
        process_link source do |hash|
            formatted = format_source hash
            if formatted.nil? || formatted[:package].nil? || formatted[:version].empty? 
                puts "whuat? hash #{hash} vs #{formatted}"
                sleep 1
                next
            end
            packages[formatted[:package]] =  formatted
            nb_source += 1
        end
        $logger.debug "Found #{nb_source} sources"

        nb_binaries = 0
        nb_mismatch = 0
        process_link binary do |hash|
            formatted = format_binary hash
            p = packages[formatted[:package]] 
            if p.nil? || p[:version] != formatted[:version]
                nb_mismatch += 1
                next
            end
            p[:hash_binary] = formatted[:hash_binary]
            nb_binaries += 1
        end
        # only select matching packages source + version
        packages.delete_if { |k,v| v[:hash_binary].nil? || v[:hash_source].nil? }
        $logger.debug "Found #{nb_binaries} binaries and #{nb_mismatch} mismatches"
        $logger.debug "Example #{packages[packages.keys.first]}"
        return Snapshot.new(time,packages)
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

    def cache_or_download link
        filen = File.join(@cache,extract_date(link) + "_" + extract_file(link))
        if !File.exists? filen
            File.open(filen,"w") do |f|
                open(link,"User-Agent" => "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.92 Safari/537.36") do |l|
                    IO.copy_stream(l,f)
                end
            end
            $logger.debug "File #{filen} has been downloaded"
        else 
            $logger.debug "File #{filen} is already cached"
        end

        File.open(filen,"r") do |f|
            reader = XZ::StreamReader.new f
            yield reader
            reader.finish
        end
    end

    ## process_link takes a link and a block and yield each paragraphs as objects.
    def process_link link
        cache_or_download link do |reader|
            parser = DebianControlParser.new reader
            parser.paragraphs do |p|
                obj = {}
                p.fields do |name,value|
                    n = name.downcase.strip.to_sym
                    obj[n] = value
                end
                if @packages && !@packages.empty? && !@packages.include?(obj[:package])
                    next
                end
                yield obj
            end
        end
    end

    def slice(hash, *keys)
        Hash[ [keys, hash.values_at(*keys)].transpose]
    end

    ## return the date compressed like YEARMONTHDAYHOURMINUTESECOND
    def extract_date link 
        p=/([0-9]{4})([0-9]{2})([0-9]{2})T([0-9]{2})([0-9]{2})([0-9]{2})Z/
        res = link.to_s.match p
        raise "no date inside" unless res
        return res.to_a[1..-1].join""
    end

    def extract_file link
        p = /\/(\w+\.[gx]z)$/
        res = link.to_s.match p
        raise "no file inside link" unless link.to_s.match p
        return $1
    end


end

