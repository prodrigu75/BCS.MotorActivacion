namespace BCS.MotorActivacion.Entities
{
    public abstract class Base
    {
        #region Member Variables
        public string mEstado;
        #endregion

        public string Estado
        {
            get { return mEstado; }
            set { mEstado = value; }
        }
    }
}
