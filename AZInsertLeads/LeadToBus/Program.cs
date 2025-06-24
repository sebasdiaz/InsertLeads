using Azure.Messaging.ServiceBus;
using LeadToBus.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.PowerPlatform.Dataverse.Client;

var host = new HostBuilder()
    .ConfigureFunctionsWorkerDefaults()
    .ConfigureServices(services =>
    {
        var crmConn = Environment.GetEnvironmentVariable("sourceCRMConn");
        var sbConn = Environment.GetEnvironmentVariable("ServiceBusConnectionString");
        var queueName = Environment.GetEnvironmentVariable("ServiceBusQueueName");

        services.AddSingleton(new ServiceClient(crmConn));

        var sbClient = new ServiceBusClient(sbConn);
        services.AddSingleton(sbClient);
        services.AddSingleton(sbClient.CreateSender(queueName));

        services.AddSingleton<ILeadFetcher, LeadFetcher>();
        services.AddSingleton<ILeadPublisher, ServiceBusPublisher>();
        services.AddSingleton<ILeadUpdater, LeadUpdater>();
    })
    .Build();

host.Run();
