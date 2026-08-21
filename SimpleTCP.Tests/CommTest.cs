using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections.Generic;
using System.Linq;

namespace SimpleTCP.Tests
{
    [TestClass]
    public class CommTest
    {
        private List<string> _clientTx = new List<string>();
        private List<string> _clientRx = new List<string>();
        private List<string> _serverRx = new List<string>();
        private List<string> _serverTx = new List<string>();


        [TestMethod]
        public void SimpleCommTest()
        {
            SimpleTcpServer server = new SimpleTcpServer().Start(8910);
            SimpleTcpClient client = new SimpleTcpClient().Connect(server.GetListeningIPs().FirstOrDefault(ip => ip.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork).ToString(), 8910);

            server.DelimiterDataReceived += (sender, msg) => {
                _serverRx.Add(msg.MessageString);
                string serverReply = Guid.NewGuid().ToString();
                msg.ReplyLine(serverReply);
                _serverTx.Add(serverReply);
            };

            client.DelimiterDataReceived += (sender, msg) => {
                _clientRx.Add(msg.MessageString);
            };

            System.Threading.Thread.Sleep(1000);

            if (server.ConnectedClientsCount == 0)
            {
                Assert.Fail("Server did not register connected client");
            }

            for (int i = 0; i < 10; i++)
            {
                string clientTxMsg = Guid.NewGuid().ToString();
                _clientTx.Add(clientTxMsg);
                client.WriteLine(clientTxMsg);
                System.Threading.Thread.Sleep(100);
            }

            System.Threading.Thread.Sleep(1000);

            for (int i = 0; i < 10; i++)
            {
                if (_clientTx[i] != _serverRx[i])
                {
                    Assert.Fail("Client TX " + i.ToString() + " did not match server RX " + i.ToString());
                }

                if (_serverTx[i] != _clientRx[i])
                {
                    Assert.Fail("Client RX " + i.ToString() + " did not match server TX " + i.ToString());
                }
            }

            var reply = client.WriteLineAndGetReply("TEST", TimeSpan.FromSeconds(1));
            if (reply == null)
            {
                Assert.Fail("WriteLineAndGetReply returned null");
            }

            Assert.IsTrue(true);
        }

        [TestMethod]
        public void ClientConnectionEventsAreRaisedOncePerTransition()
        {
            var listener = new System.Net.Sockets.TcpListener(System.Net.IPAddress.Loopback, 0);
            listener.Start();

            var endpoint = (System.Net.IPEndPoint)listener.LocalEndpoint;
            var client = new SimpleTcpClient();
            System.Net.Sockets.TcpClient serverSideClient = null;
            int started = 0;
            int interrupted = 0;
            int closed = 0;

            client.ConnectionStarted += (sender, args) => System.Threading.Interlocked.Increment(ref started);
            client.ConnectionInterrupted += (sender, args) => System.Threading.Interlocked.Increment(ref interrupted);
            client.ConnectionClosed += (sender, args) => System.Threading.Interlocked.Increment(ref closed);

            try
            {
                client.Connect(System.Net.IPAddress.Loopback.ToString(), endpoint.Port);
                serverSideClient = listener.AcceptTcpClient();

                Assert.IsTrue(client.IsConnected);
                Assert.AreEqual(1, started);

                client.Disconnect();
                System.Threading.Thread.Sleep(100);

                Assert.IsFalse(client.IsConnected);
                Assert.AreEqual(1, started);
                Assert.AreEqual(0, interrupted);
                Assert.AreEqual(1, closed);

                client.Disconnect();
                System.Threading.Thread.Sleep(50);

                Assert.AreEqual(0, interrupted);
                Assert.AreEqual(1, closed);
            }
            finally
            {
                if (serverSideClient != null) { serverSideClient.Close(); }
                client.Dispose();
                listener.Stop();
            }
        }

        [TestMethod]
        public void RemoteDisconnectRaisesConnectionInterruptedOnce()
        {
            var listener = new System.Net.Sockets.TcpListener(System.Net.IPAddress.Loopback, 0);
            listener.Start();

            var endpoint = (System.Net.IPEndPoint)listener.LocalEndpoint;
            var client = new SimpleTcpClient();
            System.Net.Sockets.TcpClient serverSideClient = null;
            int started = 0;
            int interrupted = 0;
            int closed = 0;

            client.ConnectionStarted += (sender, args) => System.Threading.Interlocked.Increment(ref started);
            client.ConnectionInterrupted += (sender, args) => System.Threading.Interlocked.Increment(ref interrupted);
            client.ConnectionClosed += (sender, args) => System.Threading.Interlocked.Increment(ref closed);

            try
            {
                client.Connect(System.Net.IPAddress.Loopback.ToString(), endpoint.Port);
                serverSideClient = listener.AcceptTcpClient();

                Assert.AreEqual(1, started);
                Assert.IsTrue(client.IsConnected);

                serverSideClient.Close();
                serverSideClient = null;

                bool interruptedDetected = System.Threading.SpinWait.SpinUntil(() => interrupted == 1, 2000);
                Assert.IsTrue(interruptedDetected, "Client did not detect the remote disconnect.");

                System.Threading.Thread.Sleep(100);

                Assert.IsFalse(client.IsConnected);
                Assert.AreEqual(1, started);
                Assert.AreEqual(1, interrupted);
                Assert.AreEqual(0, closed);
            }
            finally
            {
                if (serverSideClient != null) { serverSideClient.Close(); }
                client.Dispose();
                listener.Stop();
            }
        }
    }
}
