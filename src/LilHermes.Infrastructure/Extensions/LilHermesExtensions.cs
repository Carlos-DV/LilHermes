using System;
using LilHermes.Abstractions.Entities;
using LilHermes.Infrastructure.Connections;
using LilHermes.Infrastructure.Consumers;
using LilHermes.Infrastructure.Interfaces;
using LilHermes.Infrastructure.Publishers;
using LilHermes.Infrastructure.Telemetry;
using Microsoft.Extensions.DependencyInjection;

namespace LilHermes.Infrastructure.Extensions
{
    public static class LilHermesExtensions
    {
        /// <summary>
        /// LilHermes Full
        /// </summary>
        /// <param name="services"></param>
        /// <param name="messageOptions"></param>
        /// <returns></returns>
        public static IServiceCollection AddLilHermes(this IServiceCollection services, Action<MessageBusOptions> messageOptions)
        {
            //Config service broker
            var opt = new MessageBusOptions();
            messageOptions(opt);
            //config telemetry
            var telemetry = new LilHermesTelemetry(opt.SourceName ?? LilHermesTelemetry.DefaultSourceName);
            services.AddSingleton(opt);
            services.AddSingleton(telemetry);
            services.AddSingleton<IMessageConnectionManager ,RabbitMQConnectionManager>();
            services.AddSingleton<IMessagePublisher, RabbitMQPublisher>();
            services.AddSingleton<IMessageConsumer, RabbitMQConsumer>();
            return services;
        }
        /// <summary>
        /// LilHermes solo publicador
        /// </summary>
        /// <param name="services"></param>
        /// <param name="messageOptions"></param>
        /// <returns></returns>
        public static IServiceCollection AddLilHermesPublisher(this IServiceCollection services, Action<MessageBusOptions> messageOptions)
        {
            var opt = new MessageBusOptions();
            messageOptions(opt);
            var telemetry = new LilHermesTelemetry(opt.SourceName ?? LilHermesTelemetry.DefaultSourceName);
            services.AddSingleton(opt);
            services.AddSingleton(telemetry);
            services.AddSingleton<IMessageConnectionManager, RabbitMQConnectionManager>();
            services.AddSingleton<IMessagePublisher, RabbitMQPublisher>();
            return services;
        }
        /// <summary>
        /// LilHermes solo consumidor
        /// </summary>
        /// <param name="services"></param>
        /// <param name="messageOptions"></param>
        /// <returns></returns>
        public static IServiceCollection AddLilHermesConsumer(this IServiceCollection services, Action<MessageBusOptions> messageOptions)
        {
            var opt = new MessageBusOptions();
            messageOptions(opt);
            var telemetry = new LilHermesTelemetry(opt.SourceName ?? LilHermesTelemetry.DefaultSourceName);
            services.AddSingleton(opt);
            services.AddSingleton(telemetry);
            services.AddSingleton<IMessageConnectionManager, RabbitMQConnectionManager>();
            services.AddSingleton<IMessageConsumer, RabbitMQConsumer>();
            return services;
        }
    }
}