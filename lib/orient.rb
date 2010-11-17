require 'orient/connections/binary'

module Orient
  VERSION = '1.0.0'
end

if $0 == __FILE__
  conn = Orient::Connections::Binary.new 'localhost'
  clusters = conn.db_open 'local:demo', 'admin', 'admin'
  p conn.count clusters.find { |c| c.name == 'default' }
  #conn.datacluster_count clusters
  #conn.datacluster_datarange clusters.first
end
