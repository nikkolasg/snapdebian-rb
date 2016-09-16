require 'ostruct'
require 'thread'

require_relative'log'

Thread.abort_on_exception=true

class Snapshot

    attr_accessor :packages
    attr_accessor :time

    def initialize time,packages
        @time = Time.parse time
        @packages = packages
        @str_time = @time.strftime("%Y%m%d%H%M%S")
    end

    def <=> snap
        @time <=> snap.time
    end

    def time_format
        @str_time
    end

end

class Package
    attr_accessor :package
    attr_accessor :time_format ##
    attr_accessor :time
    attr_accessor :version
    attr_accessor :hash_source
    attr_accessor :hash_binary

    def initialize time,h
        @package = h[:package] 
        @time = Time.parse(time)
        @time_format = @time.strftime("%Y%m%d%H%M%S")
        @version = h[:version]
        @hash_source = h[:hash_source]
        @hash_binary = h[:hash_binary]
    end

    def <=> p
        @time <=> p.time 
    end

    def hash
        #[@package, @version, @hash_source, @hash_binary].hash
        [@package, @version, @hash_binary].hash
    end

    def eql? o
        (@package.eql?(o.package)) &&
            (@version.eql?(o.version)) &&
            (@hash_binary.eql?(o.hash_binary))
        #    (@hash_source.eql?(o.hash_source)) &&
    end

    def to_s
        #[@time_format,@package,@version,@hash_source,@hash_binary].join(",") + "\n"
        [@time_format,@package,@version,@hash_binary].join(",") + "\n"
    end

end

class Formatter
    @csv = "snapshots.csv"
    @checksum_field = "files".to_sym
    @source_fields = [:package,:version,:hash_source]
    @binary_fields = [:package,:version,:hash_binary]
    class << self
        attr_accessor :source_fields 
        attr_accessor :binary_fields
        attr_accessor :checksum_field
        attr_accessor :csv
    end

    require 'xz'
    require 'zlib'
    require 'debian_control_parser'
    require 'open-uri'
    require_relative 'ruby_util'
    require 'etc'
    require 'set'
    ## workaround see https://github.com/celluloid/timers/issues/20
    SortedSet.new

    def initialize folder,packages = nil,csv = nil
        @folder = folder
        Dir.mkdir(File.join(Dir.pwd,@folder)) unless File.directory?(@folder)
        @csv = File.join(@folder,Formatter.csv)
        @cache = File.join(@folder,"cache")
        Dir.mkdir @cache unless File.directory? @cache
        @packages = packages
        #@missing = %w{base-files base-passwd debconf debianutils dpkg init-system-helpers lsb sysvinit pcre3 zlib hostname netbase adduser bsdmainutils debian-archive-keyring ucf popularity-contest ifupdown mime-support libxml2 initramfs-tools ca-certificates psmisc tasksel installation-report laptop-detect linux-base xml-core os-prober discover-data dictionaries-common whois bc }
        #@missing += %w{lsb sysvinit pcre3 zlib libxml2 psmisc bzip2 bc}
        ### TODO XXX We remove thoses packages as they dont have matching
        #versions
        #@missing -= %w{lsb sysvinit pcre3 zlib libxml2 psmisc bc}
        #@packages -= %w{lsb sysvinit pcre3 zlib libxml2 psmisc bc}
        #@packages += %w{ debianutils diffutils findutils sed logrotate liblocale-gettext-perl net-tools iptables aptitude bzip2}
        #@missing.uniq!
        @missing = %w{pcre3 zlib}
        @missingFound = []
    end


    ## format takes a [time] => [binarylink,sourceLink]
    # and yields each snapshots once formatted
    def format links
        @file = File.open(@csv,"w") 
        @file.write "time, name, version, hash_source, hash_binary\n"
        @file.flush
        threads = []
        idx = 1
        RubyUtil::slice(links.keys,Etc.nprocessors) do |times|
        threads << Thread.new(times,idx) do |ttimes,i|
            Thread.current[:name] = i
            Thread.current[:packages] = []
            $logger.debug "Started thread on #{ttimes.size}/#{links.keys.size} of the snapshots"
            ttimes.each_with_index do |time,j|
                v = links[time]
                $logger.info "Processing snapshot @ #{time} (#{j}/#{ttimes.size})"
                snapshot = process_snapshot time,v[:source],v[:binary]
                Thread.current[:packages] += snapshot.packages
                # verification that all packages are there the first 
                # snapshot
                if i == 1 && j == 0
                    snapNames = snapshot.packages.map {|p| p.package }
                    set = @packages & snapNames
                    puts "Checking if first snapshot..."
                    if set.size != @packages.size
                        str = "whut? first snapshot has #{snapNames.size} vs packages list #{@packages.size}:" 
                        puts "Packages missing: \n#{(@packages-set).join(" ")}\n"
                        puts "Packages included skipped: \n#{@missingFound.join(" ")}\n"
                        raise str
                    end
                    puts "First snapshot contains everyhing!"
                end
            end
            end
            $logger.debug "Finished processing"
            idx += 1
        end
        # wait all threads
        packages =[]
        last = []
        threads.each do |t| 
            $logger.debug "Waiting on thread #{t[:name]}..."
            t.join
            packages += t[:packages]
            puts "Thread #{t[:name]} found #{t[:packages].size}"
            ## uniqueness by name+version+hashES 
            #packages.uniq! { |p| [p.package,p.version,p.hash_source,p.hash_binary]}
            packages.uniq!
            ## sort by time
            packages.sort!

            diff = (packages - last) | (last - packages)
            last = Array.new(packages)
            puts "Diff = #{diff}"
        end

        #puts "Time-files made by the threads #{.to_a}"
        packages.each { |p| @file.write p.to_s }
        @file.close
        $logger.info "Insert #{`cat #{@csv} | wc -l`.strip} lines in the #{@csv}"
    end

    private

    ## HACKYISH WAY only take binaries...
    def process_snapshot time,source,binary
        packagesStruct = []
        process_link binary do |hash|
            formatted = format_binary hash
            packagesStruct << Package.new(time,formatted)
        end
        nbBefore = packagesStruct.size
        packagesStruct.uniq! { |p| p.package }
        # only select matching packages source + version
        $logger.debug "Found #{packagesStruct.size}/#{nbBefore} binaries"
        #$logger.debug "Example #{packages[packages.keys.first]}"
        return Snapshot.new(time,packagesStruct)

    end
    ## create_snapshot takes links to source.xz file & binary.xz file. It
    #decompress them, analyzes them and return an snapshot struct
    ##def process_snapshot time,source,binary
    ##    packages = {}
    ##    packagesStruct = []
    ##    nb_source = 0
    ##    nb_wrongsources = 0
    ##    process_link source do |hash|
    ##        formatted = format_source hash
    ##        if formatted.nil? || formatted[:package].nil? || formatted[:version].empty? || formatted[:hash_source].nil?
    ##            #puts "whuat? hash #{hash} vs #{formatted}"
    ##            #sleep 1
    ##            if @missing.include? formatted[:package]
    ##                puts "Source missing package: #{formatted}"
    ##                puts "Hash origin: #{hash}"
    ##                @missingFound << hash 
    ##                sleep 10
    ##            end
    ##            nb_wrongsources += 1
    ##            next
    ##        end
    ##        packages[formatted[:package]] =  formatted
    ##        nb_source += 1
    ##    end
    ##    $logger.debug "Found #{nb_source} sources & #{nb_wrongsources} wrong format (no sha256)"

    ##    nb_binaries = 0
    ##    nb_mismatch = 0
    ##    process_link binary do |hash|
    ##        formatted = format_binary hash
    ##        p = packages[formatted[:package]] 
    ##        if p.nil? 
    ##            nb_mismatch += 1
    ##            if @missing.include? formatted[:package] 
    ##                puts "Binary missing package: #{formatted}"
    ##                puts "Source related: #{p}"
    ##                @missingFound << hash
    ##                sleep 10
    ##            end
    ##            next
    ##        elsif p[:version] != formatted[:version]
    ##            puts "Mismatch version #{formatted[:package]} #{formatted[:version]} vs #{p[:version]}"
    ##            @missingFound << hash
    ##            sleep 1
    ##            nb_mismatch += 1
    ##        
    ##            next
    ##        end

    ##        p[:hash_binary] = formatted[:hash_binary]
    ##        nb_binaries += 1
    ##        packagesStruct << Package.new(time,p)
    ##    end
    ##    # only select matching packages source + version
    ##    $logger.debug "Found #{nb_binaries} binaries and #{nb_mismatch} mismatches for #{time}"
    ##    #$logger.debug "Example #{packages[packages.keys.first]}"
    ##    return Snapshot.new(time,packagesStruct)
    ##end

    def format_source hash
        ## Multiline ...
        hash[Formatter.checksum_field].split("\n").each do |line|
            ## search for the ***.orig.tar.xz file sha256 in hexadecimal
            #followed by anything with "orig.tar" inside
            next false unless line =~ /(\w{32}).*\.(orig\.)?(tar\.[gx]z||bz2)/
            hash[:hash_source] = $1 
        end
        #hash.delete(Formatter.checksum_field)
        ## take what we need
        slice hash,*Formatter.source_fields
    end

    def format_binary hash
        if hash.include? Formatter.checksum_field
            hash[Formatter.checksum_field].split("\n").each do |line|
                ## search for the ***.orig.tar.xz file sha256 in hexadecimal
                #followed by anything with "orig.tar" inside
            next false unless line =~ /(\w{32}).*\.(orig\.)?(tar\.[gx]z||bz2)/
                hash[:hash_binary] = $1 
            end
        elsif hash.include? :md5sum
            hash[:hash_binary] = hash[:md5sum]
        else 
            puts "Searching binary for #{Formatter.checksum_field} in #{hash}"
            exit 1
        end
        #hash.delete(Formatter.checksum_field)
        slice hash,*Formatter.binary_fields
    end

    def cache_or_download link
        while true do
            begin
                $logger.debug "Trying to download #{ link.to_s}"
                filen = File.join(@cache,extract_date(link) + "_" + extract_file(link))
                if !File.exists? filen
                    ##out = `wget --quiet #{link.to_s} -O #{filen} --waitretry=2 --retry-connrefused 2>&1`
                    ##raise "error downloading file (exit #{$?})#{filen}: #{out}" unless $?.exitstatus.to_i != 0
                    while true do 
                        begin
                            File.open(filen,"w") do |f|
                                open(link,"User-Agent" => "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.92 Safari/537.36") do |l|
                                    #f.write l.read
                                    IO.copy_stream(l,f)
                                end
                            end
                            break
                        rescue OpenURI::HTTPError 
                            $logger.info "Error trying to download #{link.to_s}"
                            sleep 0.2
                            next
                        end
                    end
                    $logger.debug "File #{filen} has been downloaded"
                else 
                    $logger.debug "File #{filen} is already cached"
                end

                #File.open(filen,"r") do |f|
                #    reader = XZ::StreamReader.new f
                #    yield reader
                #    reader.finish
                #end
                Zlib::GzipReader.open(filen) do |gz|
                    yield gz
                end
                break
            rescue  Zlib::Error,Zlib::GzipFile::Error ,Zlib::GzipFile::NoFooter, Zlib::GzipFile::CRCError, Zlib::GzipFile::LengthError => e
                $logger.debug "Error downloaded #{link.to_s} => #{e}"
                File.delete(filen)
                next
            end
        end
    end


    ## process_link takes a link and a block and yield each paragraphs as objects.
    def process_link link
        nb_skipped = 0
        cache_or_download link do |reader|
            parser = DebianControlParser.new reader
            parser.paragraphs do |p|
                obj = {}
                p.fields do |name,value|
                    n = name.downcase.strip.to_sym
                    obj[n] = value.strip
                end
                if @packages && !@packages.empty? && !@packages.include?(obj[:package])
                    if @missing.include? obj[:package]
                        puts "Hey I've got one #{obj}"
                        sleep 10
                    end
                    nb_skipped += 1
                    next
                end
                yield obj
            end
        end
        $logger.debug "Skipped #{nb_skipped} packages not in the given list"
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
