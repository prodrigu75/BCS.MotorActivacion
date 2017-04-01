using Microsoft.Owin;
using Owin;

[assembly: OwinStartupAttribute(typeof(BCS.MotorActivacion.WebPresentationLayer.Startup))]
namespace BCS.MotorActivacion.WebPresentationLayer
{
    public partial class Startup {
        public void Configuration(IAppBuilder app) {
            ConfigureAuth(app);
        }
    }
}
