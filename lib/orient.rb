require 'orient/connections/binary'

module Orient
  VERSION = '1.0.0'
end

if $0 == __FILE__
  conn = Orient::Connections::Binary.new 'localhost'
  clusters = conn.db_open 'local:demo', 'admin', 'admin'
  clusters.each do |cluster|
    p cluster.name => conn.count(cluster.name)
  end

  cluster = clusters.find { |c| c.name == 'city' }
  p cluster
  conn.record_load(cluster.id, 1)
  #p conn.count clusters.find { |c| c.name == 'default' }
  #conn.datacluster_count clusters
  #conn.datacluster_datarange clusters.first
end
