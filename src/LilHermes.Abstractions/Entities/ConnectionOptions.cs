using System;
using System.Collections.Generic;

namespace LilHermes.Abstractions.Entities
{
    /// <summary>
    /// Class with the basic options to create a connection with RabbitMQ
    /// </summary>
    public class ConnectionOptions
    {
        /// <summary>
        /// Basic Connection
        /// </summary>
        public string HostName { get; set; } = "127.0.0.1";
        public int Port { get; set; } = 5672;
        public string Username { get; set; } = "guest";
        public string Password { get; set; } = "guest";
        public string VirtualHost { get; set; } = "/";
        /// <summary>
        /// Connection with URI
        /// </summary>
        public string ConnectionUri { get; set; }
        /// <summary>
        /// Identifier of channel
        /// </summary>
        public string ClientProvidedName { get; set; } = "LilHermes-Service-v0";
        /// <summary>
        /// Config Cluster
        /// </summary>
        public bool EnabledCluster { get; set; } = false;
        public List<string> Endpoints { get; set; } = new List<string>();
        /// <summary>
        /// Times and attempts
        /// </summary>
        public bool AutomaticRecoveryEnabled { get; set; } = true;
        public TimeSpan NetworkRecoveryInterval { get; set; } = TimeSpan.FromSeconds(5);
        public TimeSpan RequestedHeartbeat { get; set; } = TimeSpan.FromSeconds(60);
        public ushort RequestedChannelMax { get; set; } = 2047;
        public uint RequestedFrameMax { get; set; } = 131072;
    }
}
