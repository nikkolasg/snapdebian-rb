require_relative 'log'
require 'mechanize'

## URLs will be taken such as
# SNAPSHOT_URL / <timestamp> / PATH / {BINARY_PATH,SOURCE_PATH}
SNAPSHOT_URL = "http://snapshot.debian.org/archive/debian/"
PATH = "dists/stretch/main/"
BINARY_PATH = "binary-amd64/Packages.xz"
SOURCE_PATH = "source/Sources.xz"

module Scrapper

    class Snapshots

        def initialize packages,from,to
            @packages = packages
            @from = from
            @to = to
            @agent = Mechanize.new
            @folder = $opts[:folder] || "snapshots"
        end

        def scrap 
            agent = Mechanize.new
            flinks = links 
            $logger.info "Found #{flinks.size} snapshots falling between given dates" 
            $logger.debug "Downloading files into #{@folder}"
        end

        private 

        # take the main page and find the range links for year + months 
        # + days + hours
        def links
            from_rounded = Scrapper::round_time @from
            to_rounded = Scrapper::round_time @to
            first_links = @agent.get(SNAPSHOT_URL).links_with(href: /.\/?year=/) 
            # filter by year-month from < "year-month" > to
            first_links.select! do |links| 
                links.href =~ /.\/?year=([0-9]{4})&month=([0-9]{1,2})/
                year,month = $1,$2
                t = Time.strptime("#{year}-#{month}","%Y-%m")
                v = t >= from_rounded && t <= from_rounded
                v
            end
            seconds = first_links.inject([]) do |acc,link|
                page = link.click
                ## select all valid time links
                page.links.each do |timeLink|
                    begin
                        Time.parse timeLink.text
                        acc << timeLink
                    rescue
                        next 
                    end
                end
                acc
            end
            seconds
        end
    end

    def self.round_time time, year=true, month= true
        format = "" 
        toParse = ""
        if year
            format += "%Y" 
            toParse += time.year.to_s
        end
        if month
            format += "-%m"
            toParse += "-#{time.month.to_s}"
        end
        Time.strptime(toParse,format)
    end
end
