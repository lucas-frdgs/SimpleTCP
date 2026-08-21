using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace SimpleTCP
{
	public class SimpleTcpClient : IDisposable
	{
		public SimpleTcpClient()
		{
			StringEncoder = System.Text.Encoding.UTF8;
			ReadLoopIntervalMs = 10;
			Delimiter = 0x13;
		}

		private Thread _rxThread = null;
		private List<byte> _queuedMsg = new List<byte>();
		public byte Delimiter { get; set; }
		public System.Text.Encoding StringEncoder { get; set; }
		private TcpClient _client = null;
		private readonly object _connectionStateLock = new object();
		private bool _connectionActive;

		public event EventHandler<Message> DelimiterDataReceived;
		public event EventHandler<Message> DataReceived;
		public event EventHandler ConnectionInterrupted;
		public event EventHandler ConnectionStarted;
		public event EventHandler ConnectionClosed;

		public bool IsConnected
		{
			get { return IsSocketConnected(_client); }
		}

		private static bool IsSocketConnected(TcpClient client)
		{
			if (client == null) { return false; }

			try
			{
				Socket socket = client.Client;
				if (socket == null || !socket.Connected) { return false; }

				// TcpClient.Connected reflects the state of the last socket operation only.
				// A readable socket with no available bytes indicates that the peer closed it.
				return !(socket.Poll(0, SelectMode.SelectRead) && socket.Available == 0);
			}
			catch (ObjectDisposedException)
			{
				return false;
			}
			catch (SocketException)
			{
				return false;
			}
		}

		private void NotifyConnectionStarted(TcpClient client)
		{
			bool notify = false;

			lock (_connectionStateLock)
			{
				if (!ReferenceEquals(_client, client)) { return; }
				if (!_connectionActive)
				{
					_connectionActive = true;
					notify = true;
				}
			}

			if (notify)
			{
				ConnectionStarted?.Invoke(this, EventArgs.Empty);
			}
		}

		private void HandleConnectionInterrupted(TcpClient client)
		{
			bool notify = false;

			lock (_connectionStateLock)
			{
				if (!ReferenceEquals(_client, client)) { return; }

				_client = null;
				if (_connectionActive)
				{
					_connectionActive = false;
					notify = true;
				}
			}

			try
			{
				client.Close();
			}
			catch
			{
			}

			if (notify)
			{
				ConnectionInterrupted?.Invoke(this, EventArgs.Empty);
			}
		}

		private void HandleConnectionClosed(TcpClient client)
		{
			bool notify = false;

			lock (_connectionStateLock)
			{
				if (!ReferenceEquals(_client, client)) { return; }

				_client = null;
				if (_connectionActive)
				{
					_connectionActive = false;
					notify = true;
				}
			}

			try
			{
				client.Close();
			}
			catch
			{
			}

			if (notify)
			{
				ConnectionClosed?.Invoke(this, EventArgs.Empty);
			}
		}

		internal bool QueueStop { get; set; }
		internal int ReadLoopIntervalMs { get; set; }
		public bool AutoTrimStrings { get; set; }

		public SimpleTcpClient Connect(string hostNameOrIpAddress, int port)
		{
			if (string.IsNullOrEmpty(hostNameOrIpAddress))
			{
				throw new ArgumentNullException("hostNameOrIpAddress");
			}

			TcpClient client = new TcpClient();
			client.Connect(hostNameOrIpAddress, port);

			TcpClient previousClient = null;
			lock (_connectionStateLock)
			{
				previousClient = _client;
				_client = client;
				_connectionActive = false;
			}

			if (previousClient != null)
			{
				try
				{
					previousClient.Close();
				}
				catch
				{
				}
			}

			NotifyConnectionStarted(client);
			StartRxThread();

			return this;
		}

		private void StartRxThread()
		{
			if (_rxThread != null) { return; }

			_rxThread = new Thread(ListenerLoop);
			_rxThread.IsBackground = true;
			_rxThread.Start();
		}

		public SimpleTcpClient Disconnect()
		{
			TcpClient client = _client;
			if (client == null) { return this; }

			HandleConnectionClosed(client);
			return this;
		}

		public TcpClient TcpClient { get { return _client; } }

		private void ListenerLoop(object state)
		{
			while (!QueueStop)
			{
				try
				{
					RunLoopStep();
				}
				catch
				{
				}

				System.Threading.Thread.Sleep(ReadLoopIntervalMs);
			}

			_rxThread = null;
		}

		private void RunLoopStep()
		{
			TcpClient c = _client;
			if (c == null) { return; }

			try
			{
				if (!IsSocketConnected(c))
				{
					HandleConnectionInterrupted(c);
					return;
				}

				var delimiter = this.Delimiter;

				int bytesAvailable = c.Available;
				if (bytesAvailable == 0)
				{
					System.Threading.Thread.Sleep(10);
					return;
				}

				List<byte> bytesReceived = new List<byte>();

				while (c.Available > 0 && IsSocketConnected(c))
				{
					byte[] nextByte = new byte[1];
					int received = c.Client.Receive(nextByte, 0, 1, SocketFlags.None);
					if (received == 0)
					{
						HandleConnectionInterrupted(c);
						break;
					}

					bytesReceived.AddRange(nextByte);
					if (nextByte[0] == delimiter)
					{
						byte[] msg = _queuedMsg.ToArray();
						_queuedMsg.Clear();
						NotifyDelimiterMessageRx(c, msg);
					}
					else
					{
						_queuedMsg.AddRange(nextByte);
					}
				}

				if (bytesReceived.Count > 0)
				{
					NotifyEndTransmissionRx(c, bytesReceived.ToArray());
				}
			}
			catch (ObjectDisposedException)
			{
				HandleConnectionInterrupted(c);
			}
			catch (SocketException)
			{
				HandleConnectionInterrupted(c);
			}
		}

		private void NotifyDelimiterMessageRx(TcpClient client, byte[] msg)
		{
			if (DelimiterDataReceived != null)
			{
				Message m = new Message(msg, client, StringEncoder, Delimiter, AutoTrimStrings);
				DelimiterDataReceived(this, m);
			}
		}

		private void NotifyEndTransmissionRx(TcpClient client, byte[] msg)
		{
			if (DataReceived != null)
			{
				Message m = new Message(msg, client, StringEncoder, Delimiter, AutoTrimStrings);
				DataReceived(this, m);
			}
		}

		public void Write(byte[] data)
		{
			TcpClient client = _client;
			if (client == null) { throw new Exception("Cannot send data to a null TcpClient (check to see if Connect was called)"); }

			try
			{
				client.GetStream().Write(data, 0, data.Length);
			}
			catch (IOException)
			{
				HandleConnectionInterrupted(client);
				throw;
			}
			catch (ObjectDisposedException)
			{
				HandleConnectionInterrupted(client);
				throw;
			}
			catch (SocketException)
			{
				HandleConnectionInterrupted(client);
				throw;
			}
		}

		public void Write(string data)
		{
			if (data == null) { return; }
			Write(StringEncoder.GetBytes(data));
		}

		public void WriteLine(string data)
		{
			if (string.IsNullOrEmpty(data)) { return; }
			if (data.LastOrDefault() != Delimiter)
			{
				Write(data + StringEncoder.GetString(new byte[] { Delimiter }));
			}
			else
			{
				Write(data);
			}
		}

		public Message WriteLineAndGetReply(string data, TimeSpan timeout)
		{
			Message mReply = null;
			this.DataReceived += (s, e) => { mReply = e; };
			WriteLine(data);

			Stopwatch sw = new Stopwatch();
			sw.Start();

			while (mReply == null && sw.Elapsed < timeout)
			{
				System.Threading.Thread.Sleep(10);
			}

			return mReply;
		}


		#region IDisposable Support
		private bool disposedValue = false; // To detect redundant calls

		protected virtual void Dispose(bool disposing)
		{
			if (!disposedValue)
			{
				QueueStop = true;
				Disconnect();
				disposedValue = true;
			}
		}

		public void Dispose()
		{
			Dispose(true);
		}
		#endregion
	}
}
