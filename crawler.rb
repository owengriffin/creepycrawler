#!/bin/ruby

require 'rubygems'
require 'active_record'
require 'set'
require 'net/http'
require 'uri'
require 'logger'
require 'hpricot'

$log = Logger.new(STDOUT)
$log.level = Logger::DEBUG

# CreepyCrawler is a singleton
class CreepyCrawler
  private_class_method :new

  @@options = {
    :include_query_params => true
  }

  @@creepycrawler = nil
  def CreepyCrawler.instance
    @@creepycrawler = new unless @@creepycrawler
    @@creepycrawler
  end

  def initialize
    @queue = []
    @complete = []
  end

  def add_to_queue(url)
    @queue << url if not (@queue.include? url or @complete.include? url)
  end

  def start(urlstr)
    @queue = []
    self.add_to_queue Url.find_or_create(:url => urlstr)
    $log.debug "Starting!"
    #Thread.new {
      $log.debug @queue
      while not @queue.empty?
        self.fetch(@queue.first.url)
      end
    #}
    $log.debug "Boom!"
  end

  # Opens the database connection
  def open(file)
    ActiveRecord::Base.establish_connection({ :adapter => 'sqlite3', 
                                              :database => ':memory:' })
    ActiveRecord::Schema.define do 
      create_table :pages do |table|
        table.column :title, :string
        table.column :url_id, :integer
        table.column :parent_id, :integer
        table.column :contents, :string
      end
      create_table :redirections, :id => false do |table|
        table.column :url_id, :integer
        table.column :page_id, :integer
      end
      create_table :link_titles do |table|
        table.column :link_id, :integer
        table.column :title, :string
      end
      create_table :links do |table|
        table.column :origin_url_id, :integer
        table.column :destination_url_id, :integer
        table.column :occurances, :integer
      end
      create_table :urls do |table|
        table.column :url, :string
        table.column :status, :integer
      end
      create_table :images_titles do |table|
        table.column :image_id, :integer
        table.column :title, :string
      end
      create_table :images do |table|
        table.column :url_id, :integer
      end
      create_table :pages_images, :id => false do |table|
        table.column :image_id, :integer
        table.column :page_id, :integer
      end
    end
  end

  def fetch(url, redirect_limit = 10, urls = [])
    fail 'HTTP redirect too deep' if redirect_limit.zero?
    uri = URI.parse(url)
    path = uri.path
    path = "/" if path.empty?
    path = path + '?' + uri.query if @@options["include_query_params"]
    Net::HTTP.start(uri.host) do |http|
      $log.info "Fetching: #{path}"
      response = http.get(path)
      case response
      when Net::HTTPSuccess
        if response['content-type'] =~ /^text\/html/
          Page.parse(Url.sanatize(url), response, urls)
        else
          $log.info "Unrecognised content type #{response['content-type']}"
        end
      when Net::HTTPRedirection
        redirect_to = response['location']
        redirect_to = uri.scheme + "://" + uri.host + redirect_to if not redirect_to =~ /http[s]?/
        $log.debug "Redirecting to: #{redirect_to}"
        response = fetch(redirect_to, redirect_limit-1, urls << Url.find_or_create(:url => Url.sanatize(url), :status => response.code)) 
      else
        #response.error!
      end     
    end
  end

  def report
    for page in Page.find(:all)
      puts "Page #{page.url.url}"
      puts "Links from this page"
      Link.find(:all, :conditions => ["origin_url_id = ?", page.url.id]).each {|link|
        puts "#{link.destination.url}"
        link.titles.each {|title|
          puts "#{title.title}"
        }
      }
    end
  end
end

module Parser
  def Parser.get_document(text)
    return Hpricot(text)
  end

  def Parser.links(page, document)
    document.search('//a').each { |element|
      if not element["href"].empty?
        Parser.link(page, element)
      end
    }
  end

  def Parser.externallink(page, element)
    $log.debug "External URL #{element["href"]}"
  end

  def Parser.link(page, element)
    href = element["href"]
    if Url.is_external?(href, page.url.url)
      Parser.externallink(page, element)
    elsif Url.is_absolute? href
      href = Url.get_domain_url(page.url.url) + href[1..href.length]
    else
      href = Url.get_folder_url(page.url.url) + href[1..href.length]
    end
    
    $log.debug "Adding link to #{href}"
        
    # Lookup the destination Url entity, and create the Link entity
    destination_url = Url.find_or_create(:url => Url.sanatize(href))
    link = Link.increment_or_create(:origin_url_id => page.url.id, 
                                    :destination_url_id => destination_url.id)
    
    # If the <a> tag contains a title then add it to the list of titles
    # available for this Link entity.
    if not (element["title"] == nil or element["title"].empty?)
      link.titles << LinkTitle.create(:title => element["title"]) 
    end
    link.titles << LinkTitle.create(:title => element.inner_text)
    link.save

    # Add this link to the queue
    CreepyCrawler.instance.add_to_queue link
  end
  
  # Iterate through all the images in the provided _document_. For every
  # image a Image entity will be created
  def Parser.images(document)
    document.search('//img').each { |element|
      if not element["src"].empty?
        image_url = Url.find_or_create(:url => Url.sanatize(element["src"]))
      end
    }
  end
end

class Page < ActiveRecord::Base
  #belongs_to :parent, :class_name => "Page", :foreign_key => "parent_id"
  belongs_to :url, :foreign_key => "id"
  #has_many :pages, :class_name => "Page", :foreign_key => "parent_id"
  has_and_belongs_to_many :redirections, :join_table => "redirections", :class_name => "Url"

  def Page.parse(urlstr, http_response, redirections = [])

    # Does a page already exists with this URL?
    if Url.exists?(:url => urlstr) 
      $log.debug "#{urlstr} already exists"
      url = Url.find(:first, :conditions => ["url = ?", urlstr])
      if url.page == nil
        $log.debug "Url has no associated page"
        page = Page.create(:url => url)
        $log.debug page.inspect
      else
        $log.debug "Using URL associated with page"
        page = Page.find(:first, :conditions => ["url_id = ?", url.id])
      end
    else
      $log.debug "Creating new URL and Page"
      url = Url.create(:url => urlstr)
      page = Page.create(:url => url) 
    end
    url.save 
    page.save
    $log.debug "Page = #{page.id}"
    page.contents = http_response.body
    redirections.each { |url| 
      $log.debug("Adding redirection: #{url.url}")
      page.redirections << url
    }
    document = Parser.get_document(page.contents)
    Parser.links(page, document)
    Parser.images(document)
    page.save
    return page
  end
end

class LinkTitle < ActiveRecord::Base
  belongs_to :link
end  

class Link < ActiveRecord::Base
  has_many :titles, :class_name => "LinkTitle"
  belongs_to :origin, :class_name => "Url", :foreign_key => "origin_url_id"
  belongs_to :destination, :class_name => "Url", :foreign_key => "destination_url_id"

  # Find a link with the specified attributes. If none is found then create the link. 
  # Otherwise increment the number of occurances.
  def Link.increment_or_create(attr)
    link = nil
    if Link.exists?(attr)
      link = Link.find(:first, :conditions => attr)
      link.occurances = link.occurances + 1
      link.save
    else
      link = Link.create attr
      link.occurances = 1
    end
    return link
  end
end

class Url < ActiveRecord::Base
  belongs_to :link
  has_and_belongs_to_many :pages, :join_table => "redirections"
  belongs_to :page

  def Url.find_or_create(attr)
    if Url.exists?(attr)
      return Url.find(:first, :conditions => attr)
    else
      return Url.create(attr)
    end
  end

  def Url.strip_semicolon(href)
    return /([^;]*)/.match(href)[1]
  end

  def Url.strip_anchors(href)
    return href.split("#")[0]
  end

  # Checks to see if the given link points to an external domain.
  def Url.is_external?(href, url = "")
    retval = href =~ /^((http|ftp)(s?)|mailto)/ 
    if retval and not url.empty?
      u0 = URI.parse(url)
      u1 = URI.parse(url)
      retval = retval && u1.host == u0.host
    end
    return retval
  end

  def Url.is_absolute?(href)
    return href[0]=='/'
  end

  def Url.get_protocol(url)
    return url.match(/(?:.*)?\/{2}/)[0]
  end

  def Url.get_domain_url(url)
    # Find the first forward slash after the protocol
      uri = URI.parse(url)
      return uri.scheme + '://' + uri.host + '/'
  end

  def Url.get_folder_url(url)
    return url.match(/(.*\/)([^\/]*)/)[1]
  end

  def Url.explode(href, parent_url)
    if href[0] == '/'
      return Url.get_domain_url(parent_url) + href
    else
      #return Url.
    end
  end

  def Url.sanatize(url)
    return  Url.strip_anchors(Url.strip_semicolon(url))
  end
end

# Set ActiveRecord to log to the stdout
#ActiveRecord::Base.logger = Logger.new(STDERR)

creepycrawler = CreepyCrawler.instance
creepycrawler.open("test.db")
creepycrawler.start('http://www.betavine.net')
creepycrawler.report
