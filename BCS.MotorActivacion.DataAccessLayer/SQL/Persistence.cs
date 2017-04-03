using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.OleDb;
using System.Data.SqlClient;

public class Persistence : IDisposable
{
    public enum Provider
    {
        SQLOLEDB = 0,
        SQLCLIENT = 1,
        ORACLIENT = 2
    }

    private IDbConnection vConnection;
	private IDbTransaction vTransaction;
    private bool vDisposedValue = false;
	public bool vError = false;
    public string DescError;
    private string motor;
    
    private bool vCommited = true;

    private Provider vProvider;
    
    public string StoreProcedure;
    private List<object> vParameters;

	public void AddParameter(object pValue)
    {
        if (vParameters == null)
            vParameters = new List<object>();

        vParameters.Add(pValue);
    }

    public void Connect()
    {
        try
        {
            vConnection.Open();
            vTransaction = vConnection.BeginTransaction();
        }
        catch (Exception ex)
        {
            vError = true;
        }
    }

    public void ConnectReadUncommitted()
    {
        try
        {
            vConnection.Open();
            vTransaction = vConnection.BeginTransaction(IsolationLevel.ReadUncommitted);
        }
        catch (Exception ex)
        {
            vError = true;
        }
    }

    public void ConnectReadcommitted()
    {
        try
        {
            vConnection.Open();
            vTransaction = vConnection.BeginTransaction();
        }
        catch (Exception ex)
        {
            vError = true;
        }
    }


    public Persistence(bool pCommited, string pConnectionStringName)
    {
        string vConnectionString;

        vProvider = Provider.SQLCLIENT;
        vConnection = new SqlConnection();

        vConnectionString = ConfigurationManager.ConnectionStrings[pConnectionStringName].ConnectionString.ToString().Trim();

        if ((vConnection != null))
            if (vConnection.State != ConnectionState.Closed)
                return;

        vConnection.ConnectionString = vConnectionString;
        vCommited = pCommited;

        if (pCommited)
            ConnectReadcommitted();
        else
            ConnectReadUncommitted();
    }

    public Persistence(bool pCommited, string pConnectionStringName, Provider pProvider)
    {
        string vConnectionString;

        vProvider = pProvider;

        if (vProvider == Provider.SQLCLIENT)
            vConnection = new SqlConnection();

        if (vProvider == Provider.SQLOLEDB)
            vConnection = new OleDbConnection();

        vConnectionString = ConfigurationManager.ConnectionStrings[pConnectionStringName].ConnectionString.ToString().Trim();

        if ((vConnection != null))
            if (vConnection.State != ConnectionState.Closed)
                return;

        vConnection.ConnectionString = vConnectionString;
        vCommited = pCommited;

        if (pCommited)
            ConnectReadcommitted();
        else
            ConnectReadUncommitted();
    }
    /*
    public void ExecuteNonQuery(ref DotNetResponse.SQLPersistence Rp, string Csql)
    {
        try
        {
            if (this.vError)
            {
                Rp.MensajeError = this.DescError;
                Rp.Errores = true;
                return;
            }

            if (vProvider == Provider.SQLOLEDB)
            {
                OledbHelper.ExecuteNonquery(Rp, (OleDbTransaction)vTransaction, Csql);
            }
            if (vProvider == Provider.SQLCLIENT)
            {
                SqlClientHelper.ExecuteNonquery(Rp, (SqlTransaction)vTransaction, Csql);
            }

            if ((Rp != null))
            {
                if (Rp.Errores)
                {
                    vError = true;
                }
            }
        }
        catch (Exception ex)
        {
            //GrabarLog(ex)
            vError = true;
            try
            {
                Rp.TieneDatos = false;
                Rp.Errores = true;
                Rp.MensajeError = ex.Message;

            }
            catch (Exception exinterno)
            {
            }
        }
    }

    public void ExecuteNonquery(ref DotNetResponse.SQLPersistence Rp)
    {
        try
        {
            if (this.vError)
            {
                Rp.MensajeError = this.DescError;
                Rp.Errores = true;
                return;
            }

            if (vProvider == Provider.SQLOLEDB)
            {
                OledbHelper.ExecuteNonquery((OleDbTransaction)vTransaction, Rp.StoredProcedure, false, Rp.vParameters);
            }
            if (vProvider == Provider.SQLCLIENT)
            {
                SqlClientHelper.ExecuteNonquery((SqlTransaction)vTransaction, Rp.StoredProcedure, false, Rp.vParameters);
            }

            if ((Rp != null))
            {
                if (Rp.Errores)
                {
                    vError = true;
                }
            }

        }
        catch (Exception ex)
        {
            //GrabarLog(ex)
            vError = true;
            try
            {
                Rp.TieneDatos = false;
                Rp.Errores = true;
                Rp.MensajeError = ex.Message;

            }
            catch (Exception exinterno)
            {
            }

        }
    }

    public void ExecuteScalar(ref DotNetResponse.SQLPersistence Rp, string Csql)
    {
        try
        {
            if (this.vError)
            {
                Rp.MensajeError = this.DescError;
                Rp.Errores = true;
                return;
            }

            if (vProvider == Provider.SQLOLEDB)
            {
                OledbHelper.ExecuteScalar((OleDbTransaction)vTransaction, Csql);
            }
            if (vProvider == Provider.SQLCLIENT)
            {
                SqlClientHelper.ExecuteScalar(Rp, (SqlTransaction)vTransaction, Csql);
            }

            //End If

            if ((Rp != null))
            {
                if (Rp.GetScalar == null)
                {
                    Rp.TieneDatos = false;
                }
                else
                {
                    Rp.TieneDatos = true;
                }

                if (Rp.Errores)
                {
                    vError = true;
                }
            }


        }
        catch (Exception ex)
        {
            //  GrabarLog(ex)
            vError = true;
            try
            {
                Rp.TieneDatos = false;
                Rp.Errores = true;
                Rp.MensajeError = ex.Message;

            }
            catch (Exception exinterno)
            {
            }

        }

    }
    public void ExecuteScalar(ref DotNetResponse.SQLPersistence Rp)
    {
        try
        {
            if (this.vError)
            {
                Rp.MensajeError = this.DescError;
                Rp.Errores = true;
                return;
            }

            if (vProvider == Provider.SQLOLEDB)
            {
                Rp.GetScalar = OledbHelper.ExecuteScalar((OleDbTransaction)vTransaction, Rp.StoredProcedure, Rp.vParameters);
            }
            if (vProvider == Provider.SQLCLIENT)
            {
                Rp.GetScalar = SqlClientHelper.ExecuteScalar((SqlTransaction)vTransaction, Rp.StoredProcedure, Rp.vParameters);
            }
            if (Rp.GetScalar == null)
            {
                Rp.TieneDatos = false;
            }
            if ((Rp != null))
            {
                if (Rp.Errores)
                {
                    vError = true;
                }
            }

        }
        catch (Exception ex)
        {
            vError = true;
            try
            {
                Rp.TieneDatos = false;
                Rp.Errores = true;
                Rp.MensajeError = ex.Message;

            }
            catch (Exception exinterno)
            {
            }

        }
    }

    public void ExecuteDataTable(ref DotNetResponse.SQLPersistence Rp, string Csql)
    {
        try
        {
            if (this.vError)
            {
                Rp.MensajeError = this.DescError;
                Rp.Errores = true;
                return;
            }

            if (vProvider == Provider.SQLOLEDB)
            {
                Rp.DtTable = OledbHelper.ExecuteDataset((OleDbTransaction)vTransaction, Csql).Tables(0);
            }
            if (vProvider == Provider.SQLCLIENT)
            {
                Rp.DtTable = SqlClientHelper.ExecuteDataset((SqlTransaction)vTransaction, Csql).Tables(0);
            }

            if ((Rp != null))
            {
                if (Rp.Errores)
                {
                    vError = true;
                }
            }

        }
        catch (Exception ex)
        {
            //GrabarLog(ex)
            vError = true;
            try
            {
                Rp.TieneDatos = false;
                Rp.Errores = true;
                Rp.MensajeError = ex.Message;

            }
            catch (Exception exinterno)
            {
            }

        }
    }
    public void ExecuteDataTable(ref DotNetResponse.SQLPersistence Rp)
    {
        try
        {
            if (this.vError)
            {
                Rp.MensajeError = this.DescError;
                Rp.Errores = true;
                return;
            }

            if (vProvider == Provider.SQLOLEDB)
            {
                Rp.DtTable = OledbHelper.ExecuteDataset((OleDbTransaction)vTransaction, Rp.StoredProcedure, Rp.vParameters).Tables(0);
            }
            if (vProvider == Provider.SQLCLIENT)
            {
                Rp.DtTable = SqlClientHelper.ExecuteDataset((SqlTransaction)vTransaction, Rp.StoredProcedure, Rp.vParameters).Tables(0);
            }

            if ((Rp != null))
            {
                if (Rp.Errores)
                {
                    vError = true;
                }
            }

        }
        catch (Exception ex)
        {
            //GrabarLog(ex)
            vError = true;
            try
            {
                Rp.TieneDatos = false;
                Rp.Errores = true;
                Rp.MensajeError = ex.Message;

            }
            catch (Exception exinterno)
            {
            }

        }
    }
    public void ExecuteDataset(ref DotNetResponse.SQLPersistence Rp, string Csql)
    {
        try
        {
            if (this.vError)
            {
                Rp.MensajeError = this.DescError;
                Rp.Errores = true;
                return;
            }

            if (vProvider == Provider.SQLOLEDB)
            {
                Rp.Ds = OledbHelper.ExecuteDataset((OleDbTransaction)vTransaction, Csql);
            }
            if (vProvider == Provider.SQLCLIENT)
            {
                Rp.Ds = SqlClientHelper.ExecuteDataset((SqlTransaction)vTransaction, Csql);
            }

            if ((Rp != null))
            {
                if (Rp.Errores)
                {
                    vError = true;
                }
            }
        }
        catch (Exception ex)
        {
            //GrabarLog(ex)
            vError = true;
            try
            {
                Rp.TieneDatos = false;
                Rp.Errores = true;
                Rp.MensajeError = ex.Message;

            }
            catch (Exception exinterno)
            {
            }

        }
    }
    public void ExecuteDataset(ref DotNetResponse.SQLPersistence Rp)
    {
        try
        {
            if (this.vError)
            {
                Rp.MensajeError = this.DescError;
                Rp.Errores = true;
                return;
            }

            if (vProvider == Provider.SQLOLEDB)
            {
                Rp.Ds = OledbHelper.ExecuteDataset((OleDbTransaction)vTransaction, Rp.StoredProcedure, Rp.vParameters);
            }
            if (vProvider == Provider.SQLCLIENT)
            {
                Rp.Ds = SqlClientHelper.ExecuteDataset((SqlTransaction)vTransaction, Rp.StoredProcedure, Rp.vParameters);
            }
            if ((Rp != null))
            {
                if (Rp.Errores)
                {
                    vError = true;
                }
            }

        }
        catch (Exception ex)
        {
            //GrabarLog(ex)
            vError = true;
            try
            {
                Rp.TieneDatos = false;
                Rp.Errores = true;
                Rp.MensajeError = ex.Message;

            }
            catch (Exception exinterno)
            {
            }

        }
    }
    public void ExecuteReader(ref DotNetResponse.SQLPersistence Rp, string Csql)
    {
        try
        {
            if (this.vError)
            {
                Rp.MensajeError = this.DescError;
                Rp.Errores = true;
                return;
            }

            if (vProvider == Provider.SQLOLEDB)
            {
                Rp.GetDataReader = OledbHelper.ExecuteReader((OleDbTransaction)vTransaction, Csql);
            }
            if (vProvider == Provider.SQLCLIENT)
            {
                Rp.GetDataReader = SqlClientHelper.ExecuteReader((SqlTransaction)vTransaction, Csql);
            }
            if ((Rp != null))
            {
                Rp.TieneDatos = true;
                if (Rp.Errores)
                {
                    vError = true;
                }
            }

        }
        catch (Exception ex)
        {
            //GrabarLog(ex)
            vError = true;
            try
            {
                Rp.TieneDatos = false;
                Rp.Errores = true;
                Rp.MensajeError = ex.Message;

            }
            catch (Exception exinterno)
            {
            }

        }
    }
    public void ExecuteReader(ref DotNetResponse.SQLPersistence Rp)
    {
        try
        {
            if (this.vError)
            {
                Rp.MensajeError = this.DescError;
                Rp.Errores = true;
                return;
            }

            if (vProvider == Provider.SQLOLEDB)
            {
                Rp.GetDataReader = OledbHelper.ExecuteReader((OleDbTransaction)vTransaction, Rp.StoredProcedure, Rp.vParameters);
            }
            if (vProvider == Provider.SQLCLIENT)
            {
                Rp.GetDataReader = SqlClientHelper.ExecuteReader((SqlTransaction)vTransaction, Rp.StoredProcedure, Rp.vParameters);
            }

            if ((Rp != null))
            {
                Rp.TieneDatos = true;
                if (Rp.Errores)
                {
                    vError = true;
                }
            }


        }
        catch (Exception ex)
        {
            //GrabarLog(ex)
            vError = true;
            try
            {
                Rp.TieneDatos = false;
                Rp.Errores = true;
                Rp.MensajeError = ex.Message;

            }
            catch (Exception exinterno)
            {
            }

        }
    }
    */

    public void Rollback()
    {

        try
        {
            vTransaction.Rollback();
            Close();
        }
        catch (Exception ex)
        {
            Close();
        }

    }
    private void Commited()
    {
        if (vError == true)
        {
            try
            {
                vTransaction.Rollback();
                Close();
            }
            catch (Exception ex)
            {
            }
            return;
        }
        vTransaction.Commit();
    }
    public void Commit()
    {
        if (vError == true)
        {
            try
            {
                vTransaction.Rollback();
                Close();
            }
            catch (Exception ex)
            {
            }
            return;
        }
        if (!(vConnection.State == ConnectionState.Open))
        {
            return;
        }


        if (vCommited)
        {
            vTransaction.Commit();
        }

    }
    public void Close()
    {
        vConnection.Close();
    }

    protected virtual void Dispose(bool disposing)
    {
        if (!this.disposedValue)
        {
            if (disposing)
            {
                //TODO:              free unmanaged resources when explicitly called
            }

            //TODO: free shared unmanaged resources
        }
        this.disposedValue = true;
    }


    #region " IDisposable Support "
    // This code added by Visual Basic to correctly implement the disposable pattern.
    public void Dispose()
    {
        // Do not change this code.  Put cleanup code in Dispose(ByVal disposing As Boolean) above.
        Close();
        Dispose(true);
        GC.SuppressFinalize(this);
        GC.Collect();
    }
    #endregion

}
