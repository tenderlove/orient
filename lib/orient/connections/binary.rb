require 'socket'

module Orient
  class Cluster < Struct.new(:name, :id, :type)
  end

  module Connections
    class Binary
      RECORD_LOAD = 30
      COUNT       = 40

      def initialize remote_host, port = 2424, lhost = nil, lport = nil
        @socket = TCPSocket.new remote_host, port, lhost, lport
        @tx     = 0
      end

      def record_load cluster_id, record_id, query_plan = "*:-1"
        header = [ RECORD_LOAD, @tx ]
        header = header.pack('cN')
        packet = [
          cluster_id,
          record_id >> 32,
          record_id & 0xFFFF,
        ].pack 'nNN'

        str = [
          [ query_plan.bytesize, query_plan ],
        ]
        str = str.map { |tuple| [tuple.first].pack('N') + tuple.last }.join

        @socket.write(header + packet + str)
        response, txid = @socket.read(5).unpack('cN')
        raise unless response == 0

        @tx += 1

        status = @socket.read(1).unpack('c').first
        begin
          len    = @socket.read(4).unpack('N').first
          record = @socket.read(len)
          record_version, record_type = @socket.read(5).unpack('Nc')
          p record
          p record_type.chr
          status = @socket.read(1).unpack('c').first
        end while status == 2
      end

      def count cluster_name
        header = [ COUNT, @tx ]
        header = header.pack('cN')
        name = cluster_name
        str = [
          [ name.bytesize, name ],
        ]
        str = str.map { |tuple| [tuple.first].pack('N') + tuple.last }.join
        @socket.write(header + str)
        response, txid = @socket.read(5).unpack('cN')
        high, low = @socket.read(8).unpack('NN')
        (high << 32) + low
      end

      def datacluster_datarange cluster
        header = [ 13, @tx ]
        header = header.pack('cN')
        packet = [cluster.id].pack 'n'
        @socket.write(header + packet)

        response, txid = @socket.read(5).unpack('cN')
        raise unless response == 0

        @tx += 1
        @socket.read(8).unpack('NN')
      end

      def datacluster_count clusters
        header = [ 12, @tx ]
        header = header.pack('cN')
        packet = [ clusters.length ] + clusters.map { |x| x.id }
        packet = packet.pack("n#{clusters.length + 1}")
        @socket.write(header + packet)

        @tx += 1
        response, txid = @socket.read(5).unpack('cN')
        raise unless response == 0

        @socket.read(4).unpack('N').first
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

        response, txid = @socket.read(5).unpack('cN')
        raise unless response == 0

        connection_id, cluster_count = @socket.read(8).unpack('NN')

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
