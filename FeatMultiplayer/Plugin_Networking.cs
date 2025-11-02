// Copyright (c) David Karnok, 2023
// Licensed under the Apache License, Version 2.0

using BepInEx;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace FeatMultiplayer
{
    public partial class Plugin : BaseUnityPlugin
    {
        const int loopWakeupMillis = 500;
        const int sendBufferSize = 1024 * 1024;
        const bool fullResetBuffer = false;

        static CancellationTokenSource stopNetwork = new();
        static CancellationTokenSource stopHostAcceptor = new();

        static ConcurrentQueue<MessageBase> receiverQueue = new();

        static ClientSession hostSession;

        static int uniqueClientId;

        static readonly Dictionary<int, ClientSession> sessions = new();

        public static bool logDebugNetworkMessages;

        static NetworkTelemetry sendTelemetry = new("Send");
        static NetworkTelemetry receiveTelemetry = new("Receive");

        /// <summary>
        /// Send a message to the host.
        /// </summary>
        /// <param name="message"></param>
        /// <param name="signal"></param>
        public static void SendHost(MessageBase message, bool signal = true)
        {
            LogDebug("SendHost: " + message.GetType());
            hostSession?.Send(message, signal);
        }

        /// <summary>
        /// Send the same message to all clients.
        /// </summary>
        /// <param name="message"></param>
        /// <param name="signal"></param>
        public static void SendAllClients(MessageBase message, bool signal = true)
        {
            // No need for the log flood
            // LogDebug("SendAllClients: " + message.GetType());
            foreach (var sess in sessions.Values)
            {
                sess.Send(message, signal);
            }
        }

        /// <summary>
        /// Send the same message to all clients except one.
        /// This is needed when one client's message has to be routed to all other clients,
        /// but not back to itself.
        /// </summary>
        /// <param name="except"></param>
        /// <param name="message"></param>
        /// <param name="signal"></param>
        public static void SendAllClientsExcept(ClientSession except, MessageBase message, bool signal = true)
        {
            int id = except.id;
            LogDebug("SendAllClientsExcept: " + message.GetType() + " (except " + id + ")");
            foreach (var sess in sessions.Values)
            {
                if (sess.id != id)
                {
                    sess.Send(message, signal);
                }
            }
        }

        static void StartServer()
        {
            stopNetwork = new();
            stopHostAcceptor = new();
            Task.Factory.StartNew(HostAcceptor, TaskCreationOptions.LongRunning);
        }

        static void HostAcceptor()
        {
            var (hostIPAddress, hostDisplay) = ResolveHostAddress(hostServiceAddress.Value);
            LogInfo("Starting HostAcceptor on " + hostDisplay + ":" + hostPort.Value);
            try
            {
                TcpListener listener = new TcpListener(hostIPAddress, hostPort.Value);
                if (hostIPAddress.AddressFamily == AddressFamily.InterNetworkV6)
                {
                    try
                    {
                        listener.Server.DualMode = true;
                    }
                    catch (SocketException ex)
                    {
                        LogWarning("Failed to enable dual mode for IPv6 host listener: " + ex.Message);
                    }
                }
                listener.Start();
                stopNetwork.Token.Register(listener.Stop);
                stopHostAcceptor.Token.Register(listener.Stop);
                try
                {
                    while (!stopNetwork.IsCancellationRequested && !stopHostAcceptor.IsCancellationRequested)
                    {
                        var client = listener.AcceptTcpClient();
                        ManageClient(client);
                    }
                }
                finally
                {
                    listener.Stop();
                    LogInfo("Stopping HostAcceptor on port " + hostPort.Value);
                }
            }
            catch (Exception ex)
            {
                if (!stopNetwork.IsCancellationRequested && !stopHostAcceptor.IsCancellationRequested)
                {
                    LogError(ex);
                }
            }
        }

        static (IPAddress address, string display) ResolveHostAddress(string configured)
        {
            var value = configured?.Trim() ?? "";
            if (value.Length == 0)
            {
                var ipv4 = EnumerateLocalIPAddresses(AddressFamily.InterNetwork).Select(ip => ip.ToString()).ToList();
                var ipv6 = EnumerateLocalIPAddresses(AddressFamily.InterNetworkV6).Select(ip => ip.ToString()).ToList();
                var suffix = ipv6.Count != 0 ? "; IPv6: " + string.Join(", ", ipv6) : string.Empty;
                return (IPAddress.Any, IPAddress.Any + " (any IPv4; available: " + string.Join(", ", ipv4) + suffix + ")");
            }

            if (string.Equals(value, "auto", StringComparison.OrdinalIgnoreCase))
            {
                var ipv4 = GetMainIPv4();
                if (ipv4 != null && !IPAddress.IsLoopback(ipv4))
                {
                    return (ipv4, ipv4 + " (auto)");
                }
                return (IPAddress.Any, IPAddress.Any + " (any IPv4)");
            }

            if (string.Equals(value, "loopback", StringComparison.OrdinalIgnoreCase)
                || string.Equals(value, "localhost", StringComparison.OrdinalIgnoreCase))
            {
                return (IPAddress.Loopback, IPAddress.Loopback + " (loopback)");
            }

            if (string.Equals(value, "default", StringComparison.OrdinalIgnoreCase))
            {
                var ipv4 = GetMainIPv4();
                if (ipv4 != null && !IPAddress.IsLoopback(ipv4))
                {
                    return (ipv4, ipv4 + " (default)");
                }
                return (IPAddress.Any, IPAddress.Any + " (any IPv4)");
            }

            if (string.Equals(value, "defaultv6", StringComparison.OrdinalIgnoreCase))
            {
                var ipv6 = GetMainIPv6();
                if (ipv6 != null && !IPAddress.IsLoopback(ipv6))
                {
                    return (ipv6, ipv6 + " (defaultv6)");
                }
                return (IPAddress.IPv6Any, IPAddress.IPv6Any + " (any IPv6)");
            }

            if (IPAddress.TryParse(value, out var parsed))
            {
                return (parsed, parsed.ToString());
            }

            LogWarning("Invalid host ServiceAddress '" + configured + "', falling back to IPv4 any");
            return (IPAddress.Any, IPAddress.Any + " (any IPv4)");
        }

        static void ManageClient(TcpClient client)
        {
            LogDebug("Accepting client from " + client.Client.RemoteEndPoint);
            var session = new ClientSession(Interlocked.Increment(ref uniqueClientId));
            session.tcpClient = client;
            sessions.Add(session.id, session);

            Task.Factory.StartNew(() => ReceiverLoop(session), TaskCreationOptions.LongRunning);
            Task.Factory.StartNew(() => SenderLoop(session), TaskCreationOptions.LongRunning);
        }

        static void StartClient()
        {
            stopNetwork = new();
            hostSession = new ClientSession(0);
            hostSession.clientName = ""; // host

            Task.Run(ClientRunner);
        }

        static void ClientRunner()
        {
            var (effectiveAddress, displayAddress) = ResolveClientAddress();
            LogInfo("Client connecting to " + effectiveAddress + ":" + clientPort.Value + " (configured " + displayAddress + ")");

            try
            {
                TcpClient client;
                if (IPAddress.TryParse(effectiveAddress, out var parsed) && parsed.AddressFamily == AddressFamily.InterNetworkV6)
                {
                    client = new TcpClient(AddressFamily.InterNetworkV6);
                    try
                    {
                        client.Client.DualMode = true;
                    }
                    catch (SocketException ex)
                    {
                        LogWarning("Failed to enable dual mode for IPv6 client socket: " + ex.Message);
                    }
                }
                else
                {
                    client = new TcpClient();
                }
                hostSession.tcpClient = client;


                stopNetwork.Token.Register(client.Close);

                hostSession.tcpClient.Connect(effectiveAddress, clientPort.Value);
                LogInfo("Client connection success");

                Task.Factory.StartNew(() => ReceiverLoop(hostSession), TaskCreationOptions.LongRunning);
                Task.Factory.StartNew(() => SenderLoop(hostSession), TaskCreationOptions.LongRunning);
            }
            catch (Exception ex)
            {
                var msg = new MessageLoginResponse();
                msg.reason = "Error_ConnectionRefused";
                receiverQueue.Enqueue(msg);

                if (!stopNetwork.IsCancellationRequested)
                {
                    LogError(ex);
                }
            }
        }

        static (string effectiveAddress, string displayAddress) ResolveClientAddress()
        {
            var value = clientConnectAddress.Value?.Trim() ?? "";
            if (value.Length == 0 || string.Equals(value, "localhost", StringComparison.OrdinalIgnoreCase))
            {
                var loopback = IPAddress.Loopback.ToString();
                return (loopback, loopback + " (loopback)");
            }

            if (string.Equals(value, "loopback", StringComparison.OrdinalIgnoreCase))
            {
                var loopback = IPAddress.Loopback.ToString();
                return (loopback, loopback + " (loopback)");
            }

            if (string.Equals(value, "auto", StringComparison.OrdinalIgnoreCase))
            {
                var auto = GetMainIPv4();
                if (auto != null && !IPAddress.IsLoopback(auto))
                {
                    return (auto.ToString(), auto + " (auto)");
                }
                var loopback = IPAddress.Loopback.ToString();
                return (loopback, loopback + " (auto)");
            }

            if (string.Equals(value, "default", StringComparison.OrdinalIgnoreCase))
            {
                var ipv4 = GetMainIPv4();
                if (ipv4 != null)
                {
                    return (ipv4.ToString(), ipv4 + " (default)");
                }
            }

            if (string.Equals(value, "defaultv6", StringComparison.OrdinalIgnoreCase))
            {
                var ipv6 = GetMainIPv6();
                if (ipv6 != null)
                {
                    return (ipv6.ToString(), ipv6 + " (defaultv6)");
                }
                var loopback = IPAddress.IPv6Loopback.ToString();
                return (loopback, loopback + " (defaultv6)");
            }

            if (IPAddress.TryParse(value, out var parsed) && IPAddress.IsLoopback(parsed))
            {
                var text = parsed.ToString();
                return (text, text + " (loopback)");
            }

            return (value, value);
        }

        static void SenderLoop(ClientSession session)
        {
            sendTelemetry.stopWatch.Start();

            var tcpClient = session.tcpClient;
            var stream = tcpClient.GetStream();

            LogDebug("SenderLoop Start for session " + session.id + " from " + tcpClient.Client.RemoteEndPoint);
            try
            {
                try
                {
                    var encodeBuffer = new MemoryStream(sendBufferSize);
                    var encodeWriter = new BinaryWriter(encodeBuffer, Encoding.UTF8);

                    while (!stopNetwork.IsCancellationRequested && !session.disconnectToken.IsCancellationRequested)
                    {
                        if (session.senderQueue.TryDequeue(out var msg))
                        {
                            if (msg == MessageDisconnect.Instance)
                            {
                                LogDebug("SenderLoop for session " + session.id + " < " + session.clientName + " > send message " + msg.MessageCode());
                                break;
                            }

                            if (logDebugNetworkMessages)
                            {
                                LogDebug("SenderLoop for session " + session.id + " < " + session.clientName + " > send message " + msg.MessageCode());
                            }
                            ClearMemoryStream(encodeBuffer, fullResetBuffer);

                            var code = msg.MessageCodeBytes();

                            // placeholder for the header sizes
                            encodeWriter.Write(0); // length of the message code + 1 for its size + the length of the message body
                            encodeWriter.Write((byte)code.Length);
                            encodeWriter.Write(code);

                            var pos = encodeBuffer.Position;
                            msg.Encode(encodeWriter);

                            var msgLength = (int)(encodeBuffer.Position - pos);
                            var messageTotalLength = code.Length + msgLength;
                            encodeBuffer.Position = 0;
                            encodeWriter.Write(messageTotalLength);
                            encodeBuffer.Position = 0;

                            if (logDebugNetworkMessages)
                            {
                                LogDebug("    Message sizes: 4 + 1 + " + code.Length + " + " + msgLength + " ?= " + encodeBuffer.Length);
                                StringBuilder sb = new();
                                sb.Append("        ");
                                for (int i = 0; i < 5 + code.Length; i++)
                                {
                                    sb.Append(string.Format("{0:000} ", encodeBuffer.GetBuffer()[i]));
                                }
                                LogDebug(sb.ToString());
                            }
                            sendTelemetry.AddTelemetry(msg.MessageCode(), messageTotalLength + 5);


                            encodeBuffer.WriteTo(stream);
                            stream.Flush();

                            if (logDebugNetworkMessages)
                            {
                                LogDebug("    Sent.");
                            }
                        }
                        else
                        {
                            session.signal.WaitOne(loopWakeupMillis); // wake up anyway
                        }
                    }
                }
                finally
                {
                    SessionTerminate(session);
                    tcpClient.Close();
                }
            }
            catch (Exception ex)
            {
                if (!stopNetwork.IsCancellationRequested && !session.disconnectToken.IsCancellationRequested)
                {
                    LogError("Crash in SenderLoop for session " + session.id + " < " + session.clientName + " > from "
                        + tcpClient.Client.RemoteEndPoint + "\r\n" + ex);
                }
            }
        }

        static void ReceiverLoop(ClientSession session)
        {
            receiveTelemetry.stopWatch.Start();

            var tcpClient = session.tcpClient;
            var stream = tcpClient.GetStream();
            session.disconnectToken.Register(tcpClient.Close);

            LogDebug("ReceiverLoop Start for client " + session.id + " from " + session.tcpClient.Client.RemoteEndPoint);
            try
            {
                try
                {
                    var encodeBuffer = new MemoryStream(sendBufferSize);
                    var encodeReader = new BinaryReader(encodeBuffer);

                    while (!stopNetwork.IsCancellationRequested && !session.disconnectToken.IsCancellationRequested)
                    {
                        ClearMemoryStream(encodeBuffer, fullResetBuffer);
                        encodeBuffer.SetLength(5);

                        // Read the header with the total and the message code lengths
                        var read = ReadFully(stream, encodeBuffer.GetBuffer(), 0, 5);
                        if (read != 5)
                        {
                            throw new IOException("ReceiverLoop expected 5 more bytes but got " + read);
                        }

                        if (logDebugNetworkMessages)
                        {
                            LogDebug("ReceiverLoop message incoming");
                            StringBuilder sb = new();
                            sb.Append("        ");
                            for (int i = 0; i < 5; i++)
                            {
                                sb.Append(string.Format("{0:000} ", encodeBuffer.GetBuffer()[i]));
                            }
                            LogDebug(sb.ToString());
                        }


                        var totalLength = encodeReader.ReadInt32();
                        var messageCodeLen = encodeReader.ReadByte();

                        if (logDebugNetworkMessages)
                        {
                            LogDebug("    Message totalLength = " + totalLength + ", messageCodeLen = " + messageCodeLen);
                        }

                        // make sure the buffer can hold the remaining of the message
                        if (encodeBuffer.Capacity < 5 + totalLength)
                        {
                            encodeBuffer.Capacity = 5 + totalLength;
                        }

                        encodeBuffer.SetLength(5 + totalLength);
                        // read the rest
                        read = ReadFully(stream, encodeBuffer.GetBuffer(), 5, totalLength);

                        // broken stream?
                        if (read != totalLength)
                        {
                            throw new IOException("ReceiverLoop expected " + totalLength + " more bytes but got " + read);
                        }

                        // make the buffer appear to hold all.
                        encodeBuffer.Position = 5;

                        // decode the the messageCode
                        var messageCode = Encoding.UTF8.GetString(encodeBuffer.GetBuffer(), 5, messageCodeLen);
                        encodeBuffer.Position = 5 + messageCodeLen;

                        if (logDebugNetworkMessages)
                        {
                            LogDebug("    Code: " + messageCode + " with length 4 + 1 + " + messageCodeLen + " + " + (totalLength - messageCodeLen));
                        }
                        receiveTelemetry.AddTelemetry(messageCode, totalLength + 5);

                        // lookup an actual code decoder
                        if (messageRegistry.TryGetValue(messageCode, out var msg))
                        {
                            try
                            {
                                if (msg.TryDecode(encodeReader, out var decoded))
                                {
                                    if (decoded.GetType() != msg.GetType())
                                    {
                                        LogError("    Decoder type bug. Expected = " + msg.GetType() + ", Actual = " + decoded.GetType());
                                    }
                                    else
                                    {
                                        if (logDebugNetworkMessages)
                                        {
                                            LogDebug("    Decode complete.");
                                        }
                                        decoded.sender = session;
                                        decoded.onReceive = msg.onReceive;
                                        receiverQueue.Enqueue(decoded);
                                    }
                                }
                                else
                                {
                                    LogWarning("ReceiverLoop failed to decode message of " + messageCode + " via " + msg.GetType());
                                }
                            } 
                            catch (Exception ex)
                            {
                                LogError("    Decoder crash " + msg.GetType() + "\r\n" + ex);
                            }
                        }
                        else
                        {
                            LogWarning("ReceiverLoop unsupported message type: " + messageCode);
                        }
                    }
                } 
                finally
                {
                    LogDebug("ReceiverLoop ending for " + session.id + " from " + session.tcpClient.Client.RemoteEndPoint);
                    SessionTerminate(session);
                    tcpClient.Close();
                    LogDebug("ReceiverLoop ended for " + session.id + " from " + session.tcpClient.Client.RemoteEndPoint);
                }
            }
            catch (Exception ex)
            {
                if (!stopNetwork.IsCancellationRequested && !session.disconnectToken.IsCancellationRequested)
                {
                    LogError("Crash in ReceiverLoop for client " + session.id + " < " + session.clientName + " > from "
                        + session.tcpClient.Client.RemoteEndPoint + "\r\n" + ex);
                }
            }
        }

        static int ReadFully(Stream stream, byte[] buffer, int offset, int length)
        {
            int read;
            int remaining = length;
            while (remaining > 0)
            {
                read = stream.Read(buffer, offset, remaining);
                if (read <= 0)
                {
                    break;
                }
                offset += read;
                remaining -= read;
            }
            return length - remaining;
        }

        static void ClearMemoryStream(MemoryStream ms, bool fullClear)
        {
            if (fullClear)
            {
                var arr = ms.GetBuffer();
                Array.Clear(arr, 0, arr.Length);
            }
            ms.Position = 0;
            ms.SetLength(0);
        }

        static IEnumerable<IPAddress> EnumerateLocalIPAddresses(AddressFamily family)
        {
            List<IPAddress> result = new();
            HashSet<string> seen = new();

            foreach (var networkInterface in NetworkInterface.GetAllNetworkInterfaces())
            {
                if (networkInterface.OperationalStatus != OperationalStatus.Up)
                {
                    continue;
                }

                if (networkInterface.NetworkInterfaceType == NetworkInterfaceType.Loopback)
                {
                    continue;
                }

                var properties = networkInterface.GetIPProperties();
                foreach (var unicastAddress in properties.UnicastAddresses)
                {
                    var address = unicastAddress.Address;
                    if (address.AddressFamily != family)
                    {
                        continue;
                    }

                    if (family == AddressFamily.InterNetwork)
                    {
                        var bytes = address.GetAddressBytes();
                        if (bytes.Length > 1 && bytes[0] == 169 && bytes[1] == 254)
                        {
                            continue;
                        }
                    }

                    if (family == AddressFamily.InterNetworkV6 && address.IsIPv6LinkLocal)
                    {
                        continue;
                    }

                    var key = address.ToString();
                    if (seen.Add(key))
                    {
                        result.Add(address);
                    }
                }
            }

            if (result.Count == 0)
            {
                if (family == AddressFamily.InterNetwork)
                {
                    result.Add(IPAddress.Loopback);
                }
                else if (family == AddressFamily.InterNetworkV6)
                {
                    result.Add(IPAddress.IPv6Loopback);
                }
            }

            return result;
        }

        static IPAddress GetMainIPv4() => EnumerateLocalIPAddresses(AddressFamily.InterNetwork).FirstOrDefault();

        static IPAddress GetMainIPv6() => EnumerateLocalIPAddresses(AddressFamily.InterNetworkV6).FirstOrDefault();


        
    }

    /// <summary>
    /// Represents all information regarding a connecting or connected client.
    /// </summary>
    public class ClientSession
    {
        public readonly int id;

        public volatile string clientName;

        public volatile bool loginSuccess;

        public volatile bool disconnected;

        public TcpClient tcpClient;

        public readonly CancellationToken disconnectToken = new();

        internal readonly ConcurrentQueue<MessageBase> senderQueue = new();

        internal readonly AutoResetEvent signal = new(false);

        /// <summary>
        /// Remembers how may days worth of GPlanet.dailyXXX has been sent over during the full sync.
        /// </summary>
        internal int planetDataSync;

        public ClientSession(int id)
        {
            this.id = id;
        }

        public void Send(MessageBase message, bool signal = true)
        {
            if (!disconnected)
            {
                senderQueue.Enqueue(message);
                if (signal)
                {
                    this.signal.Set();
                }
            }
        }
    }

    
}
