using System.Collections.Generic;

namespace BCS.MotorActivacion.Entities
{
    public class Ramos: List<Ramo>
    {
    }

    public class Ramo: Base
    {
        public short mCodigoRamo;
        public string mNombre;
        public string mDescripcion;

        public short CodigoRamo
        {
            get { return mCodigoRamo; }
            set { mCodigoRamo = value; }
        }

        public string Nombre
        {
            get { return mNombre; }
            set { mNombre = value; }
        }

        public string Descripcion
        {
            get { return mDescripcion; }
            set { mDescripcion = value; }
        }
    }
}
