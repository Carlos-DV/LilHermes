using FluentAssertions;
using LilHermes.Infrastructure.Telemetry;

namespace LilHermes.UnitTests
{
    public class LilHermesTelemetryTests
    {
        private static readonly string AssemblyVersion =
            typeof(LilHermesTelemetry).Assembly.GetName().Version!.ToString(3);

        [Fact]
        public void DefaultConstructor_UsesDefaultSourceName()
        {
            var telemetry = new LilHermesTelemetry();

            LilHermesTelemetry.DefaultSourceName.Should().Be("LilHermes");
            telemetry.ActivitySource.Name.Should().Be(LilHermesTelemetry.DefaultSourceName);
            telemetry.Meter.Name.Should().Be(LilHermesTelemetry.DefaultSourceName);
        }

        [Fact]
        public void ActivitySourceAndMeter_UsePackageVersion()
        {
            var telemetry = new LilHermesTelemetry("LilHermes.VersionTest");

            telemetry.ActivitySource.Version.Should().Be(AssemblyVersion);
            telemetry.Meter.Version.Should().Be(AssemblyVersion);
        }
    }
}
