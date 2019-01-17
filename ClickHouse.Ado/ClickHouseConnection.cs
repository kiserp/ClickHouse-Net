using System;
#if !NETCOREAPP11
using System.Data;
#endif
using System.IO;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using ClickHouse.Ado.Impl;
using ClickHouse.Ado.Impl.Data;

namespace ClickHouse.Ado
{
    public class ClickHouseConnection
#if !NETCOREAPP11
        : IDbConnection
#endif
    {
        private bool UseSSL { get; set; } = false;

        private static bool _SslValidationPassThrough { get; set; } = false;

        public X509Certificate ServerCertificate { get; private set; }

        public ClickHouseConnectionSettings ConnectionSettings { get; private set; }

        public ClickHouseConnection()
        {
        }

        public ClickHouseConnection(ClickHouseConnectionSettings settings)
        {
            ConnectionSettings = settings;
        }
        public ClickHouseConnection(string connectionString)
        {
            ConnectionSettings = new ClickHouseConnectionSettings(connectionString);
        }

        public ClickHouseConnection(string connectionString, bool useSSL, bool sslValidationPassthrough = false)
        {
            ConnectionSettings = new ClickHouseConnectionSettings(connectionString);
            //ServerCertificate = X509Certificate.CreateFromCertFile(certPath);
            UseSSL = useSSL;
            _SslValidationPassThrough = sslValidationPassthrough;
        }
        private TcpClient _tcpClient;
        private Stream _stream;
        private Stream _bufferedStream;
        /*private BinaryReader _reader;
        private BinaryWriter _writer;*/
        internal ProtocolFormatter Formatter { get;
            set; }
        private NetworkStream _netStream;

        public void Dispose()
        {
            if (_tcpClient != null) Close();
        }

        public void Close()
        {
            /*if (_reader != null)
            {
                _reader.Close();
                _reader.Dispose();
                _reader = null;
            }
            if (_writer != null)
            {
                _writer.Close();
                _writer.Dispose();
                _writer = null;
            }*/
            if (_stream != null)
            {
#if !NETSTANDARD15 && !NETCOREAPP11
				_stream.Close();
#endif
				_stream.Dispose();
                _stream = null;
            }
            if (_bufferedStream != null)
            {
#if !NETSTANDARD15 &&!NETCOREAPP11
				_bufferedStream.Close();
#endif
                _bufferedStream.Dispose();
                _bufferedStream = null;
            }
            if (_netStream != null)
            {
#if !NETSTANDARD15 &&!NETCOREAPP11
				_netStream.Close();
#endif
				_netStream.Dispose();
                _netStream = null;
            }
            if (_tcpClient != null)
            {
#if !NETSTANDARD15 && !NETCOREAPP11
				_tcpClient.Close();
#else
				_tcpClient.Dispose();
#endif
				_tcpClient = null;
            }
            if (Formatter != null)
            {
                Formatter.Close();
                Formatter = null;
            }
        }


        public static bool ValidateServerCertificate(
              object sender,
              X509Certificate certificate,
              X509Chain chain,
              SslPolicyErrors sslPolicyErrors)
        {
            
            if (sslPolicyErrors == SslPolicyErrors.None || _SslValidationPassThrough)
            {
                return true;
            }
            return false;
        }

        public void Open()
        {
            if(_tcpClient!=null)throw new InvalidOperationException("Connection already open.");
            _tcpClient=new TcpClient();
            _tcpClient.ReceiveTimeout = ConnectionSettings.SocketTimeout;
            _tcpClient.SendTimeout = ConnectionSettings.SocketTimeout;
            //_tcpClient.NoDelay = true;
            _tcpClient.ReceiveBufferSize = ConnectionSettings.BufferSize;
            _tcpClient.SendBufferSize = ConnectionSettings.BufferSize;
#if NETCOREAPP11
            _tcpClient.ConnectAsync(ConnectionSettings.Host, ConnectionSettings.Port).Wait();
#elif NETSTANDARD15
            _tcpClient.ConnectAsync(ConnectionSettings.Host, ConnectionSettings.Port).ConfigureAwait(false).GetAwaiter().GetResult();
#else
			_tcpClient.Connect(ConnectionSettings.Host, ConnectionSettings.Port);
#endif
            _netStream = new NetworkStream(_tcpClient.Client);

            if (!UseSSL)
            {
                _bufferedStream = new BufferedStream(_netStream);
                _stream = new UnclosableStream(_bufferedStream);
            }
            else
            {
                var sslStream = new SslStream(_netStream, 
                                                false,
                                                new RemoteCertificateValidationCallback(ValidateServerCertificate),
                                                null
                                                );
                try
                {

                    sslStream.AuthenticateAsClient(ConnectionSettings.Host);
                }
                catch (Exception ex)
                {
                    _tcpClient.Close();
                    throw ex;
                }
                _stream = new UnclosableStream(sslStream);
            }
            //_stream = new UnclosableStream(_bufferedStream);
            /*_reader=new BinaryReader(new UnclosableStream(_stream));
            _writer=new BinaryWriter(new UnclosableStream(_stream));*/
            var ci=new ClientInfo();
            ci.InitialAddress = ci.CurrentAddress = _tcpClient.Client.RemoteEndPoint;
            ci.PopulateEnvironment();

            Formatter = new ProtocolFormatter(_stream,ci, ()=>_tcpClient.Client.Poll(ConnectionSettings.SocketTimeout, SelectMode.SelectRead));
            Formatter.Handshake(ConnectionSettings);
        }

        public string ConnectionString
        {
            get { return ConnectionSettings.ToString(); }
            set { ConnectionSettings=new ClickHouseConnectionSettings(value);}
        }

        public int ConnectionTimeout { get; set; }
        public string Database { get; private set; }
#if !NETCOREAPP11
        public ConnectionState State => Formatter != null ? ConnectionState.Open : ConnectionState.Closed;

        public IDbTransaction BeginTransaction()
        {
            throw new NotSupportedException();
        }

        public IDbTransaction BeginTransaction(IsolationLevel il)
        {
            throw new NotSupportedException();
        }
        IDbCommand IDbConnection.CreateCommand()
        {
            return new ClickHouseCommand(this);
        }
#endif
        public void ChangeDatabase(string databaseName)
        {
            CreateCommand("USE " + ProtocolFormatter.EscapeName(databaseName)).ExecuteNonQuery();
            Database=databaseName;
        }

        public ClickHouseCommand CreateCommand()
        {
            return new ClickHouseCommand(this);
        }
        public ClickHouseCommand CreateCommand(string text)
        {
            return new ClickHouseCommand(this,text);
        }
    }
}
