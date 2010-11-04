require 'socket'

module Orient
  class Cluster < Struct.new(:name, :id, :type)
  end

  module Connections
    class Binary
      def initialize remote_host, port = 2424, lhost = nil, lport = nil
        @socket = TCPSocket.new remote_host, port, lhost, lport
        @tx     = 0
      end

      def datacluster_count cluster
      end

      def db_open db, user, pass
        header = [
          5, # Operation: CONNECT
          @tx, # TX ID
        ]
        header = header.pack('cN')
        str = [
          [ db.bytesize, db ],
          [ user.bytesize, user ],
          [ pass.bytesize, pass ]
        ]
        str = str.map { |tuple| [tuple.first].pack('N') + tuple.last }.join
        @socket.write(header + str)
        @tx += 1

        response = @socket.read(1).unpack('c').first

        raise unless response == 0

        txid, connection_id, cluster_count = @socket.read(12).unpack('N3')

        clusters = []
        cluster_count.times do
          len             = @socket.read(4).unpack('N').first
          cluster_name    = @socket.read(len)
          cluster_id, len = @socket.read(8).unpack('N2')
          cluster_type    = @socket.read(len)
          clusters << Cluster.new(cluster_name, cluster_id, cluster_type)
        end

        clusters
      end
    end
  end
end
