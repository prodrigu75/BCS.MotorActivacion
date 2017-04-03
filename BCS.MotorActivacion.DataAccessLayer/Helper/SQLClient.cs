using System;
using System.Collections;
using System.Data;
using System.Data.SqlClient;

namespace BCS.MotorActivacion.DataAccessLayer.Helper
{
    internal sealed class SqlClient
    {
        #region "private utility methods & constructors"

        /// <summary>
        /// Since this class provides only static methods, make the default constructor private to prevent 
        /// instances from being created with "newSqlClientHelper()".
        /// </summary>
        private SqlClient()
        {
        }

        /// <summary>
        /// This method is used to attach array of SqlParameters to a SqlCommand.
        /// This method will assign a value of DbNull to any parameter with a direction of
        /// InputOutput and a value of null.  
        /// This behavior will prevent default values from being used, but
        /// this will be the less common case than an intended pure output parameter (derived as InputOutput)
        /// where the user provided no input value.
        /// </summary>
        /// <param name="pCommand">The command to which the parameters will be added</param>
        /// <param name="pCommandParameters">An array of SqlParameters to be added to command</param>
        private static void AttachParameters(SqlCommand pCommand, SqlParameter[] pCommandParameters)
        {
            if (pCommand == null)
                throw new ArgumentNullException("pCommand");

            if (pCommandParameters != null)
            {
                foreach (SqlParameter tmpSqlParameter in pCommandParameters)
                {
                    if (((tmpSqlParameter != null)))
                    {
                        SqlParameter vSqlParameter = default(SqlParameter);

                        // Check for derived output value with no value assigned
                        if ((tmpSqlParameter.Direction == ParameterDirection.InputOutput || tmpSqlParameter.Direction == ParameterDirection.Input) && tmpSqlParameter.Value == null)
                            p.Value = DBNull.Value;

                        pCommand.Parameters.Add(vSqlParameter);
                    }
                }
            }
        }

        /// <summary>
        /// This method assigns dataRow column values to An array of SqlParameters.
        /// </summary>
        /// <param name="pCommandParameters">Array of SqlParameters to be assigned values</param>
        /// <param name="pDataRow">The dataRow used to hold the stored procedure' s parameter values</param>
        private static void AssignParameterValues(SqlParameter[] pCommandParameters, DataRow pDataRow)
        {
            // Do nothing if we get no data    
            if (pCommandParameters == null || pDataRow == null)
                return;

            int i = 0;

            foreach (SqlParameter tmpCommandParameter in pCommandParameters)
            {
                // Set the parameters values
                SqlParameter vCommandParameter = default(SqlParameter);

                // Check the parameter name
                if ((tmpCommandParameter.ParameterName == null || tmpCommandParameter.ParameterName.Length <= 1))
                    throw new Exception(string.Format("Please provide A valid parameter name on the parameter #{0}, the ParameterName property has the following value: ' {1}' .", i, tmpCommandParameter.ParameterName));

                if (pDataRow.Table.Columns.IndexOf(tmpCommandParameter.ParameterName.Substring(1)) != -1)
                    vCommandParameter.Value = pDataRow[tmpCommandParameter.ParameterName.Substring(1)];

                i = i + 1;
            }
        }

        /// <summary>
        /// This method assigns An array of values to An array of SqlParameters.
        /// </summary>
        /// <param name="pCommandParameters">An array of SqlParameters to be assigned values</param>
        /// <param name="pParameterValues">An array of objects holding the values to be assigned</param>
        private static void AssignParameterValues(SqlParameter[] pCommandParameters, object[] pParameterValues)
        {
            int i = 0;
            int j = 0;

            // Do nothing if we get no data
            if ((pCommandParameters == null) && (pParameterValues == null))
                return;

            // We must have the same number of values as we pave parameters to put them in
            if (pCommandParameters.Length != pParameterValues.Length)
                throw new ArgumentException("Parameter count does not match Parameter Value count.");

            // Value array
            j = pCommandParameters.Length - 1;

            for (i = 0; i <= j; i++)
            {
                // If the current array value derives from IDbDataParameter, then assign its Value property
                if (pParameterValues[i] is IDbDataParameter)
                {
                    IDbDataParameter paramInstance = (IDbDataParameter)pParameterValues[i];

                    if ((paramInstance.Value == null))
                        pCommandParameters[i].Value = DBNull.Value;
                    else
                        pCommandParameters[i].Value = paramInstance.Value;
                }
                else if ((pParameterValues[i] == null))
                    pCommandParameters[i].Value = DBNull.Value;
                else
                    pCommandParameters[i].Value = pCommandParameters[i];
            }
        }

        /// <summary>
        /// This method opens (if necessary) and assigns a connection, transaction, command type and parameters 
        /// to the provided command.
        /// </summary>
        /// <param name="pSqlCommand">The SqlCommand to be prepared</param>
        /// <param name="pSqlConnection">A valid SqlConnection, on which to execute this command</param>
        /// <param name="pSqlTransaction">A valid SqlTransaction, or ' null'</param>
        /// <param name="pCommandType">The CommandType (stored procedure, text, etc.)</param>
        /// <param name="pCommandText">The stored procedure name or T-Sql command</param>
        /// <param name="pSqlCommandParameters">An array of SqlParameters to be associated with the command or ' null' if no parameters are required</param>
        /// <param name="pMustCloseConnection">Indicates when a connection should close</param>
        private static void PrepareCommand(SqlCommand pSqlCommand, SqlConnection pSqlConnection, SqlTransaction pSqlTransaction, CommandType pCommandType, string pCommandText, SqlParameter[] pSqlCommandParameters, ref bool pMustCloseConnection)
        {
            if ((pSqlCommand == null))
                throw new ArgumentNullException("pSqlCommand");

            if (string.IsNullOrEmpty(pCommandText))
                throw new ArgumentNullException("pCommandText");

            // If the provided connection is not open, we will open it
            if (pSqlConnection.State != ConnectionState.Open)
            {
                pSqlConnection.Open();
                pMustCloseConnection = true;
            }
            else
                pMustCloseConnection = false;

            // Associate the connection with the command
            pSqlCommand.Connection = pSqlConnection;
            pSqlCommand.CommandTimeout = pSqlConnection.ConnectionTimeout;
            // Set the command text (stored procedure name or Sql statement)
            pSqlCommand.CommandText = pCommandText;

            // If we were provided a transaction, assign it.
            if ((pSqlTransaction != null))
            {
                if (pSqlTransaction.Connection == null)
                    throw new ArgumentException("The transaction was rollbacked or commited, please provide an open transaction.", "pSqlTransaction");

                pSqlCommand.Transaction = pSqlTransaction;
            }

            // Set the command type
            pSqlCommand.CommandType = pCommandType;

            // Attach the command parameters if they are provided
            if ((pSqlCommandParameters != null))
                AttachParameters(pSqlCommand, pSqlCommandParameters);

            return;
        }
        #endregion

        public static string ExecuteScalar(ref Persistence pPersistence, SqlTransaction pSqlTransaccion, string pSqlString)
        {
            string vReturnValue = null;

            try
            {
                SqlCommand vSqlCommand = new SqlCommand();
                vSqlCommand.CommandText = pSqlString;
                vSqlCommand.CommandType = CommandType.Text;
                vSqlCommand.CommandTimeout = 100;
                vSqlCommand.Connection = pSqlTransaccion.Connection;
                vSqlCommand.Transaction = pSqlTransaccion;

                dynamic Paso_Valor = vSqlCommand.ExecuteScalar();

                if (Paso_Valor == null)
                {
                    pPersistence.GetScalar() = null;
                    pPersistence.TieneDatos = false;
                    vReturnValue = null;
                }
                else
                {
                    pPersistence.GetScalar = Paso_Valor;
                    pPersistence.TieneDatos = true;
                    vReturnValue = Paso_Valor;
                }

                vSqlCommand.Dispose();
            }
            catch (Exception ex)
            {
                pPersistence.MensajeError = ex.Message;
                pPersistence.TieneDatos = false;
                vReturnValue = null;
            }

            return vReturnValue;
        }

        #region "ExecuteNonQuery"
        public static object ExecuteNonquery(ref Persistence pPersistence, SqlTransaction pSqlTransaccion, string pSqlString)
        {
            bool vReturnValue = false;

            try
            {
                SqlCommand vSqlCommand = new SqlCommand();
                vSqlCommand.CommandText = pSqlString;
                vSqlCommand.CommandType = CommandType.Text;
                vSqlCommand.CommandTimeout = 30;
                vSqlCommand.Connection = pSqlTransaccion.Connection;
                vSqlCommand.Transaction = pSqlTransaccion;
                vSqlCommand.ExecuteNonQuery();
                vSqlCommand.Dispose();

                vReturnValue = true;
            }
            catch (Exception ex)
            {
                pPersistence.TieneDatos = false;
                pPersistence.Errores = true;
                pPersistence.MensajeError = ex.Message;
            }

            return vReturnValue;
        }

        /// <summary>
        /// Execute a SqlCommand (that returns no resultset and takes no parameters) against the database specified in 
        /// the connection string. 
        /// e.g.:  
        /// int result = ExecuteNonQuery(connString, CommandType.StoredProcedure, "PublishOrders");
        /// </summary>
        /// <param name="pConnectionString">A valid connection string for a SqlConnection</param>
        /// <param name="pCommandType">The CommandType (stored procedure, text, etc.)</param>
        /// <param name="pCommandText">The stored procedure name or T-Sql command</param>
        /// <returns>An int representing the number of rows affected by the command</returns>
        public static int ExecuteNonQuery(string pConnectionString, CommandType pCommandType, string pCommandText)
        {
            // Pass through the call providing null for the set of SqlParameters
            return ExecuteNonquery(pConnectionString, pCommandType, pCommandText, null);
        }

        // ExecuteNonQuery

        // Execute a SqlCommand (that returns no resultset) against the database specified in the connection string 
        // using the provided parameters.
        // e.g.:  
        // Dim result As Integer = ExecuteNonQuery(connString, CommandType.StoredProcedure, "PublishOrders", new SqlParameter("@prodid", 24))
        // Parameters:
        // -connectionString - A valid connection string for a SqlConnection
        // -commandType - The CommandType (stored procedure, text, etc.)
        // -pCommandText - The stored procedure name or T-Sql command
        // -commandParameters - An array of SqlParamters used to execute the command
        // Returns: An int representing the number of rows affected by the command
        public static int ExecuteNonQuery(string pConnectionString, CommandType pCommandType, string pCommandText, params SqlParameter[] pCommandParameters)
        {
            if (string.IsNullOrEmpty(pConnectionString))
                throw new ArgumentNullException("pConnectionString");

            // Create & open a SqlConnection, and dispose of it after we are done
            SqlConnection vSqlConnection = null;

            try
            {
                vSqlConnection = new SqlConnection(pConnectionString);
                vSqlConnection.Open();

                // Call the overload that takes a connection in place of the connection string
                return ExecuteNonquery(vSqlConnection, pCommandType, pCommandText, pCommandParameters);
            }
            finally
            {
                if (vSqlConnection != null)
                    vSqlConnection.Dispose();
            }
        }

        // Execute a stored procedure via a SqlCommand (that returns no resultset) against the database specified in 
        // the connection string using the provided parameter values.  This method will discover the parameters for the 
        // stored procedure, and assign the values based on parameter order.
        // This method provides no access to output parameters or the stored procedure' s return value parameter.
        // e.g.:  
        //  Dim result As Integer = ExecuteNonQuery(connString, "PublishOrders", 24, 36)
        // Parameters:
        // -connectionString - A valid connection string for a SqlConnection
        // -pStoredProcedureName - the name of the stored procedure
        // -pParameterValues - An array of objects to be assigned as the input values of the stored procedure
        // Returns: An int representing the number of rows affected by the command
        public static int ExecuteNonQuery(string pConnectionString, string pStoredProcedureName, params object[] pParameterValues)
        {

            if (string.IsNullOrEmpty(pConnectionString))
                throw new ArgumentNullException("pConnectionString");

            if (string.IsNullOrEmpty(pStoredProcedureName))
                throw new ArgumentNullException("pStoredProcedureName");

            SqlParameter[] vCommandParameters = null;

            // If we receive parameter values, we need to figure out where they go
            if ((pParameterValues != null) && pParameterValues.Length > 0)
            {
                // Pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)

                vCommandParameters = SqlClientParameterCache.GetSpParameterSet(pConnectionString, pStoredProcedureName);

                // Assign the provided values to these parameters based on parameter order
                AssignParameterValues(vCommandParameters, pParameterValues);

                // Call the overload that takes An array of SqlParameters
                return ExecuteNonquery(pConnectionString, CommandType.StoredProcedure, pStoredProcedureName, vCommandParameters);
                // Otherwise we can just call the SP without params
            }
            else
            {
                return ExecuteNonquery(pConnectionString, CommandType.StoredProcedure, pStoredProcedureName);
            }
        }

        /// <summary>
        /// Execute a SqlCommand (that returns no resultset and takes no parameters) against the provided SqlConnection. 
        /// e.g.:  
        /// int result = ExecuteNonQuery(conn, CommandType.StoredProcedure, "PublishOrders");
        /// </summary>
        /// <param name="pSqlConnection">A valid SqlConnection</param>
        /// <param name="pCommandType">The CommandType (stored procedure, text, etc.)</param>
        /// <param name="pCommandText">The stored procedure name or T-Sql command</param>
        /// <returns>An int representing the number of rows affected by the command</returns>
        public static int ExecuteNonQuery(SqlConnection pSqlConnection, CommandType pCommandType, string pCommandText)
        {
            // Pass through the call providing null for the set of SqlParameters
            return ExecuteNonquery(pSqlConnection, pCommandType, pCommandText, null);

        }

        /// <summary>
        /// Execute a SqlCommand (that returns no resultset) against the specified SqlConnection 
        /// using the provided parameters.
        /// e.g.:  
        /// int result = ExecuteNonQuery(conn, CommandType.StoredProcedure, "PublishOrders", new SqlParameter("@prodid", 24));
        /// </summary>
        /// <param name="pSqlConnection">A valid SqlConnection</param>
        /// <param name="pCommandType">The command type (stored procedure, text, etc.)</param>
        /// <param name="pCommandText">The stored procedure name or T-Sql command </param>
        /// <param name="pCommandParameters">An array of SqlParamters used to execute the command</param>
        /// <returns>An int representing the number of rows affected by the command</returns>
        public static int ExecuteNonQuery(SqlConnection pSqlConnection, CommandType pCommandType, string pCommandText, params SqlParameter[] pCommandParameters)
        {
            if ((pSqlConnection == null))
                throw new ArgumentNullException("pSqlConnection");

            // Create a command and prepare it for execution
            SqlCommand vSqlCommand = new SqlCommand();

            int vReturnValue = 0;
            bool vMustCloseConnection = false;

            PrepareCommand(vSqlCommand, pSqlConnection, (SqlTransaction)null, pCommandType, pCommandText, pCommandParameters, ref vMustCloseConnection);
            vSqlCommand.CommandTimeout = 500;

            // Finally, execute the command
            vReturnValue = vSqlCommand.ExecuteNonQuery();

            // Detach the SqlParameters from the command object, so they can be used again
            vSqlCommand.Parameters.Clear();

            if (vMustCloseConnection)
                pSqlConnection.Close();

            return vReturnValue;
        }

        // Execute a stored procedure via a SqlCommand (that returns no resultset) against the specified SqlConnection 
        // using the provided parameter values.  This method will discover the parameters for the 
        // stored procedure, and assign the values based on parameter order.
        // This method provides no access to output parameters or the stored procedure' s return value parameter.
        // e.g.:  
        //  Dim result As integer = ExecuteNonQuery(conn, "PublishOrders", 24, 36)
        // Parameters:
        // -connection - A valid SqlConnection
        // -pStoredProcedureName - the name of the stored procedure 
        // -pParameterValues - An array of objects to be assigned as the input values of the stored procedure 
        // Returns: An int representing the number of rows affected by the command 
        public static int ExecuteNonQuery(SqlConnection pSqlConnection, string pStoredProcedureName, params object[] pParameterValues)
        {
            if ((pSqlConnection == null))
                throw new ArgumentNullException("pSqlConnection");

            if (string.IsNullOrEmpty(pStoredProcedureName))
                throw new ArgumentNullException("pStoredProcedureName");

            SqlParameter[] vCommandParameters = null;

            // If we receive parameter values, we need to figure out where they go
            if ((pParameterValues != null) && pParameterValues.Length > 0)
            {
                // Pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                vCommandParameters = SqlClientParameterCache.GetSpParameterSet(pSqlConnection, pStoredProcedureName);

                // Assign the provided values to these parameters based on parameter order
                AssignParameterValues(vCommandParameters, pParameterValues);

                // Call the overload that takes An array of SqlParameters
                return ExecuteNonquery(pSqlConnection, CommandType.StoredProcedure, pStoredProcedureName, vCommandParameters);
                // Otherwise we can just call the SP without params
            }
            else
            {
                return ExecuteNonquery(pSqlConnection, CommandType.StoredProcedure, pStoredProcedureName);
            }

        }

        // Execute a SqlCommand (that returns no resultset and takes no parameters) against the provided SqlTransaction.
        // e.g.:  
        //  Dim result As Integer = ExecuteNonQuery(trans, CommandType.StoredProcedure, "PublishOrders")
        // Parameters:
        // -transaction - A valid SqlTransaction associated with the connection 
        // -commandType - The CommandType (stored procedure, text, etc.) 
        // -pCommandText - The stored procedure name or T-Sql command 
        // Returns: An int representing the number of rows affected by the command 
        public static int ExecuteNonQuery(SqlTransaction pSqlTransaction, CommandType pCommandType, string pCommandText)
        {
            // Pass through the call providing null for the set of SqlParameters
            return ExecuteNonquery(transaction, commandType, pCommandText, (SqlParameter[])null);
        }

        /// <summary>
        /// Execute a SqlCommand (that returns no resultset) against the specified SqlTransaction
        /// using the provided parameters.
        /// e.g.:  
        /// 
        /// int result = ExecuteNonQuery(trans, CommandType.StoredProcedure, "GetOrders", new SqlParameter("@prodid", 24));
        /// </summary>
        /// <param name="pSqlTransaction">A valid SqlTransaction</param>
        /// <param name="pCommandType">The CommandType (stored procedure, text, etc.)</param>
        /// <param name="pCommandText">The stored procedure name or T-Sql command</param>
        /// <param name="pCommandParameters">An array of SqlParamters used to execute the command</param>
        /// <returns>An int representing the number of rows affected by the command</returns>
        public static int ExecuteNonQuery(SqlTransaction pSqlTransaction, CommandType pCommandType, string pCommandText, params SqlParameter[] pCommandParameters)
        {
            if (pSqlTransaction == null)
                throw new ArgumentNullException("pSqlTransaction");

            if (pSqlTransaction != null && pSqlTransaction.Connection == null)
                throw new ArgumentException("The transaction was rollbacked or commited, please provide an open transaction.", "pSqlTransaction");

            // Create a command and prepare it for execution
            SqlCommand vSqlCommand = new SqlCommand();
            int vReturnValue = 0;
            bool vMustCloseConnection = false;

            PrepareCommand(vSqlCommand, pSqlTransaction.Connection, pSqlTransaction, pCommandType, pCommandText, pCommandParameters, ref vMustCloseConnection);

            // Finally, execute the command
            vSqlCommand.CommandTimeout = 500;
            vReturnValue = vSqlCommand.ExecuteNonQuery();

            // Detach the SqlParameters from the command object, so they can be used again
            vSqlCommand.Parameters.Clear();

            return vReturnValue;
        }

        // Execute a stored procedure via a SqlCommand (that returns no resultset) against the specified SqlTransaction 
        // using the provided parameter values.  This method will discover the parameters for the 
        // stored procedure, and assign the values based on parameter order.
        // This method provides no access to output parameters or the stored procedure' s return value parameter.
        // e.g.:  
        // Dim result As Integer =SqlClient.ExecuteNonQuery(trans, "PublishOrders", 24, 36)
        // Parameters:
        // -transaction - A valid SqlTransaction 
        // -pStoredProcedureName - the name of the stored procedure 
        // -pParameterValues - An array of objects to be assigned as the input values of the stored procedure 
        // Returns: An int representing the number of rows affected by the command 
        public static int ExecuteNonQuery(SqlTransaction pSqlTransaction, string pStoredProcedureName, bool pReturnParam, params object[] pParameterValues)
        {
            if (pSqlTransaction == null)
                throw new ArgumentNullException("pSqlTransaction");

            if (pSqlTransaction != null && pSqlTransaction.Connection == null)
                throw new ArgumentException("The transaction was rollbacked or commited, please provide an open transaction.", "pSqlTransaction");

            if (string.IsNullOrEmpty(pStoredProcedureName))
                throw new ArgumentNullException("pStoredProcedureName");

            SqlParameter[] vCommandParameters = null;

            // If we receive parameter values, we need to figure out where they go
            if ((pParameterValues != null) && pParameterValues.Length > 0)
            {
                // Pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                vCommandParameters = SqlClientParameterCache.GetSpParameterSet(pSqlTransaction.Connection, pStoredProcedureName, pReturnParam);

                // Assign the provided values to these parameters based on parameter order
                AssignParameterValues(vCommandParameters, pParameterValues);

                // Call the overload that takes An array of SqlParameters
                return ExecuteNonquery(pSqlTransaction, CommandType.StoredProcedure, pStoredProcedureName, vCommandParameters);
                // Otherwise we can just call the SP without params
            }
            else
                return ExecuteNonquery(pSqlTransaction, CommandType.StoredProcedure, pStoredProcedureName);
        }
        #endregion

        #region "ExecuteDataset"
        public static DataSet ExecuteDataset(SqlTransaction pSqlTransaccion, string pSqlString)
        {
            DataSet vReturnValue = default(DataSet);
            SqlCommand vSqlCommand = default(SqlCommand);

            DataSet vDataSet = default(DataSet);
            SqlDataAdapter vSqlDataAdapter = default(SqlDataAdapter);

            vSqlCommand = new SqlCommand();
            vSqlCommand.CommandText = pSqlString;
            vSqlCommand.CommandType = CommandType.Text;
            vSqlCommand.CommandTimeout = 30;
            vSqlCommand.Connection = pSqlTransaccion.Connection;
            vSqlCommand.Transaction = pSqlTransaccion;
            vDataSet = new DataSet();
            vSqlDataAdapter = new SqlDataAdapter();

            vSqlDataAdapter.SelectCommand = vSqlCommand;
            vSqlDataAdapter.Fill(vDataSet, "Nodes");
            vReturnValue = vDataSet;
            vDataSet.Dispose();
            vSqlDataAdapter.Dispose();
            vSqlCommand.Dispose();

            return vReturnValue;
        }

        public static DataSet ExecuteDataset(string pConnectionString, CommandType pCommandType, string pCommandText)
        {
            // Pass through the call providing null for the set of SqlParameters
            return ExecuteDataset(pConnectionString, pCommandType, pCommandText, (SqlParameter[])null);
        }

        /// <summary>
        /// Execute a SqlCommand (that returns a resultset) against the database specified in the connection string 
        /// using the provided parameters.
        /// e.g.:  
        /// Dataset ds = ExecuteDataset(connString, CommandType.StoredProcedure, "GetOrders", new SqlParameter("@prodid", 24));
        /// </summary>
        /// <param name="pConnectionString">A valid connection string for a SqlConnection</param>
        /// <param name="pCommandType">The CommandType (stored procedure, text, etc.)</param>
        /// <param name="pCommandText">The stored procedure name or T-Sql command</param>
        /// <param name="pCommandParameters">An array of SqlParamters used to execute the command</param>
        /// <returns>A dataset containing the resultset generated by the command</returns>
        public static DataSet ExecuteDataset(string pConnectionString, CommandType pCommandType, string pCommandText, params SqlParameter[] pCommandParameters)
        {
            if (string.IsNullOrEmpty(pConnectionString))
                throw new ArgumentNullException("pConnectionString");

            // Create & open a SqlConnection, and dispose of it after we are done
            SqlConnection vSqlConnection = null;

            try
            {
                vSqlConnection = new SqlConnection(pConnectionString);
                vSqlConnection.Open();

                // Call the overload that takes a connection in place of the connection string
                return ExecuteDataset(vSqlConnection, pCommandType, pCommandText, pCommandParameters);
            }
            finally
            {
                if ((vSqlConnection != null))
                    vSqlConnection.Dispose();
            }
        }

        // Execute a stored procedure via a SqlCommand (that returns a resultset) against the database specified in 
        // the connection string using the provided parameter values.  This method will discover the parameters for the 
        // stored procedure, and assign the values based on parameter order.
        // This method provides no access to output parameters or the stored procedure' s return value parameter.
        // e.g.:  
        // Dim ds As Dataset= ExecuteDataset(connString, "GetOrders", 24, 36)
        // Parameters:
        // -connectionString - A valid connection string for a SqlConnection
        // -pStoredProcedureName - the name of the stored procedure
        // -pParameterValues - An array of objects to be assigned as the input values of the stored procedure
        // Returns: A dataset containing the resultset generated by the command
        public static DataSet ExecuteDataset(string connectionString, string pStoredProcedureName, params object[] pParameterValues)
        {

            if (string.IsNullOrEmpty(pConnectionString))
                throw new ArgumentNullException("pConnectionString");
            if (string.IsNullOrEmpty(pStoredProcedureName))
                throw new ArgumentNullException("pStoredProcedureName");
            SqlParameter[] commandParameters = null;

            // If we receive parameter values, we need to figure out where they go
            if ((pParameterValues != null) && pParameterValues.Length > 0)
            {
                // Pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                commandParameters = SqlClientParameterCache.GetSpParameterSet(connectionString, pStoredProcedureName);

                // Assign the provided values to these parameters based on parameter order
                AssignParameterValues(commandParameters, pParameterValues);

                // Call the overload that takes An array of SqlParameters
                return ExecuteDataset(connectionString, CommandType.StoredProcedure, pStoredProcedureName, commandParameters);
                // Otherwise we can just call the SP without params
            }
            else
            {
                return ExecuteDataset(connectionString, CommandType.StoredProcedure, pStoredProcedureName);
            }
        }
        // ExecuteDataset

        /// <summary>
        /// Execute a SqlCommand (that returns a resultset and takes no parameters) against the provided SqlConnection. 
        /// e.g.:  
        /// Dataset ds = ExecuteDataset(conn, CommandType.StoredProcedure, "GetOrders");
        /// </summary>
        /// <param name="pSqlConnection">A valid SqlConnection</param>
        /// <param name="pCommandType">The CommandType (stored procedure, text, etc.)</param>
        /// <param name="pCommandText">The stored procedure name or T-Sql command</param>
        /// <returns>A dataset containing the resultset generated by the command</returns>
        public static DataSet ExecuteDataset(SqlConnection pSqlConnection, CommandType pCommandType, string pCommandText)
        {
            // Pass through the call providing null for the set of SqlParameters
            return ExecuteDataset(pSqlConnection, pCommandType, pCommandText, null);
        }

        /// <summary>
        /// Execute a SqlCommand (that returns a resultset) against the specified SqlConnection 
        /// using the provided parameters.
        /// e.g.:  
        /// Dataset ds = ExecuteDataset(conn, CommandType.StoredProcedure, "GetOrders", new SqlParameter("@prodid", 24));
        /// </summary>
        /// <param name="pSqlConnection">A valid SqlConnection</param>
        /// <param name="pCommandType">The CommandType (stored procedure, text, etc.)</param>
        /// <param name="pCommandText">The stored procedure name or T-Sql command</param>
        /// <param name="pCommandParameters">An array of SqlParamters used to execute the command</param>
        /// <returns>A dataset containing the resultset generated by the command</returns>
        public static DataSet ExecuteDataset(SqlConnection pSqlConnection, CommandType pCommandType, string pCommandText, params SqlParameter[] pCommandParameters)
        {
            if (pSqlConnection == null)
                throw new ArgumentNullException("pSqlConnection");

            // Create a command and prepare it for execution
            SqlCommand vSqlCommand = new SqlCommand();
            DataSet vDataSet = new DataSet();
            SqlDataAdapter vSqlDataAdatpter = null;
            bool vMustCloseConnection = false;

            PrepareCommand(vSqlCommand, pSqlConnection, (SqlTransaction)null, pCommandType, pCommandText, pCommandParameters, ref vMustCloseConnection);
            vSqlCommand.CommandTimeout = 500;

            try
            {
                // Create the DataAdapter & DataSet
                vSqlCommand.CommandTimeout = 500;
                vSqlDataAdatpter = new SqlDataAdapter(vSqlCommand);

                // Fill the DataSet using default values for DataTable names, etc
                vSqlDataAdatpter.Fill(vDataSet);

                // Detach the SqlParameters from the command object, so they can be used again
                vSqlCommand.Parameters.Clear();
            }
            finally
            {
                if (vSqlDataAdatpter != null)
                    vSqlDataAdatpter.Dispose();
            }

            if (vMustCloseConnection)
                pSqlConnection.Close();

            // Return the dataset
            return vDataSet;
        }

        /// <summary>
        /// Execute a stored procedure via a SqlCommand (that returns a resultset) against the specified SqlConnection 
        /// using the provided parameter values.  This method will discover the parameters for the 
        /// stored procedure, and assign the values based on parameter order.
        /// This method provides no access to output parameters or the stored procedure' s return value parameter.
        /// e.g.:  
        /// Dataset ds = ExecuteDataset(conn, "GetOrders", 24, 36);
        /// </summary>
        /// <param name="pSqlConnection"> A valid SqlConnection</param>
        /// <param name="pStoredProcedureName">The name of the stored procedure</param>
        /// <param name="pParameterValues">An array of objects to be assigned as the input values of the stored procedure</param>
        /// <returns>A dataset containing the resultset generated by the command</returns>
        public static DataSet ExecuteDataset(SqlConnection pSqlConnection, string pStoredProcedureName, params object[] pParameterValues)
        {

            if (pSqlConnection == null)
                throw new ArgumentNullException("pSqlConnection");

            if (string.IsNullOrEmpty(pStoredProcedureName))
                throw new ArgumentNullException("pStoredProcedureName");

            SqlParameter[] vCommandParameters = null;

            // If we receive parameter values, we need to figure out where they go

            if ((pParameterValues != null) && pParameterValues.Length > 0)
            {
                // Pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                vCommandParameters = SqlClientParameterCache.GetSpParameterSet(pSqlConnection, pStoredProcedureName);

                // Assign the provided values to these parameters based on parameter order
                AssignParameterValues(vCommandParameters, pParameterValues);

                // Call the overload that takes An array of SqlParameters
                return ExecuteDataset(pSqlConnection, CommandType.StoredProcedure, pStoredProcedureName, vCommandParameters);
                // Otherwise we can just call the SP without params
            }
            else
                return ExecuteDataset(pSqlConnection, CommandType.StoredProcedure, pStoredProcedureName);
        }

        // Parameters
        // -transaction - 
        // -commandType - The CommandType (stored procedure, text, etc.)
        // -pCommandText - The stored procedure name or T-Sql command
        // Returns: A dataset containing the resultset generated by the command
        /// <summary>
        /// Execute a SqlCommand (that returns a resultset and takes no parameters) against the provided SqlTransaction. 
        /// e.g.:  
        /// Dataset ds = ExecuteDataset(trans, CommandType.StoredProcedure, "GetOrders");
        /// </summary>
        /// <param name="pSqlTransaction">A valid SqlTransaction</param>
        /// <param name="pCommandType"></param>
        /// <param name="pCommandText"></param>
        /// <returns></returns>
        public static DataSet ExecuteDataset(SqlTransaction pSqlTransaction, CommandType pCommandType, string pCommandText)
        {
            // Pass through the call providing null for the set of SqlParameters
            return ExecuteDataset(pSqlTransaction, pCommandType, pCommandText, null);
        }

        // Execute a SqlCommand (that returns a resultset) against the specified SqlTransaction
        // using the provided parameters.
        // e.g.:  
        // Dim ds As Dataset = ExecuteDataset(trans, CommandType.StoredProcedure, "GetOrders", new SqlParameter("@prodid", 24))
        // Parameters
        // -transaction - A valid SqlTransaction 
        // -commandType - The CommandType (stored procedure, text, etc.)
        // -pCommandText - The stored procedure name or T-Sql command
        // -commandParameters - An array of SqlParamters used to execute the command
        // Returns: A dataset containing the resultset generated by the command
        public static DataSet ExecuteDataset(SqlTransaction pSqlTransaction, CommandType pCommandType, string pCommandText, params SqlParameter[] commandParameters)
        {
            if (pSqlTransaction == null)
                throw new ArgumentNullException("pSqlTransaction");
            if ((pSqlTransaction != null) && (pSqlTransaction.Connection == null))
                throw new ArgumentException("The transaction was rollbacked or commited, please provide an open transaction.", "transaction");

            // Create a command and prepare it for execution
            SqlCommand vSqlCommand = new SqlCommand();
            DataSet ds = new DataSet();
            SqlDataAdapter dataAdatpter = null;
            bool vMustCloseConnection = false;

            PrepareCommand(vSqlCommand, transaction.Connection, transaction, commandType, pCommandText, commandParameters, ref vMustCloseConnection);
            vSqlCommand.CommandTimeout = 500;

            try
            {
                // Create the DataAdapter & DataSet
                vSqlCommand.CommandTimeout = 500;
                dataAdatpter = new SqlDataAdapter(vSqlCommand);

                // Fill the DataSet using default values for DataTable names, etc
                dataAdatpter.Fill(ds);

                // Detach the SqlParameters from the command object, so they can be used again
                vSqlCommand.Parameters.Clear();
            }
            finally
            {
                if (((dataAdatpter != null)))
                    dataAdatpter.Dispose();
            }

            // Return the dataset
            return ds;

        }
        // ExecuteDataset

        /// <summary>
        /// Execute a stored procedure via a SqlCommand (that returns a resultset) against the specified
        /// SqlTransaction using the provided parameter values.  This method will discover the parameters for the 
        /// stored procedure, and assign the values based on parameter order.
        /// This method provides no access to output parameters or the stored procedure' s return value parameter.
        /// e.g.:  
        /// Dataset ds = ExecuteDataset(trans, "GetOrders", 24, 36);
        /// </summary>
        /// <param name="pSqlTransaction"> A valid SqlTransaction</param>
        /// <param name="pStoredProcedureName">The name of the stored procedure</param>
        /// <param name="pParameterValues">An array of objects to be assigned as the input values of the stored procedure</param>
        /// <returns>A dataset containing the resultset generated by the command</returns>
        public static DataSet ExecuteDataset(SqlTransaction pSqlTransaction, string pStoredProcedureName, params object[] pParameterValues)
        {
            if ((pSqlTransaction == null))
                throw new ArgumentNullException("pSqlTransaction");

            if ((pSqlTransaction != null) && (pSqlTransaction.Connection == null))
                throw new ArgumentException("The transaction was rollbacked or commited, please provide an open transaction.", "pSqlTransaction");

            if (string.IsNullOrEmpty(pStoredProcedureName))
                throw new ArgumentNullException("pStoredProcedureName");

            SqlParameter[] vCommandParameters = null;

            // If we receive parameter values, we need to figure out where they go
            if ((pParameterValues != null) && pParameterValues.Length > 0)
            {
                // Pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                vCommandParameters = SqlClientParameterCache.GetSpParameterSet(pSqlTransaction.Connection, pStoredProcedureName);

                // Assign the provided values to these parameters based on parameter order
                AssignParameterValues(vCommandParameters, pParameterValues);

                // Call the overload that takes An array of SqlParameters
                return ExecuteDataset(pSqlTransaction, CommandType.StoredProcedure, pStoredProcedureName, vCommandParameters);
                // Otherwise we can just call the SP without params
            }
            else
                return ExecuteDataset(pSqlTransaction, CommandType.StoredProcedure, pStoredProcedureName);
        }
        #endregion

        #region "ExecuteReader"
        /// <summary>
        /// This enum is used to indicate whether the connection was provided by the caller, or created bySqlClientHelper, so that
        /// we can set the appropriate CommandBehavior when calling ExecuteReader()
        /// </summary>
        private enum SqlConnectionOwnership
        {
            // Connection is owned and managed bySqlClientHelper
            Internal,
            // Connection is owned and managed by the caller
            External
        }

        /// <summary>
        /// Create and prepare a SqlCommand, and call ExecuteReader with the appropriate CommandBehavior.
        /// </summary>
        /// <param name="pSqlTransaccion">A valid SqlTransaction, or ' null'</param>
        /// <param name="pSqlString">A t-sql query</param>
        /// <returns></returns>
        public static SqlDataReader ExecuteReader(SqlTransaction pSqlTransaccion, string pSqlString)
        {
            SqlCommand vSqlCommand = default(SqlCommand);

            vSqlCommand = new SqlCommand();

            vSqlCommand.CommandText = pSqlString;
            vSqlCommand.CommandType = CommandType.Text;
            vSqlCommand.CommandTimeout = 30;
            vSqlCommand.Connection = pSqlTransaccion.Connection;
            vSqlCommand.Transaction = pSqlTransaccion;

            return vSqlCommand.ExecuteReader(CommandBehavior.CloseConnection);
        }

        /// <summary>
        /// Create and prepare a SqlCommand, and call ExecuteReader with the appropriate CommandBehavior.
        /// If we created and opened the connection, we want the connection to be closed when the DataReader is closed.
        /// If the caller provided the connection, we want to leave it to them to manage.
        /// </summary>
        /// <param name="pSqlConnection">A valid SqlConnection, on which to execute this command</param>
        /// <param name="pSqlTransaction">A valid SqlTransaction, or ' null'</param>
        /// <param name="pCommandType">The CommandType (stored procedure, text, etc.)</param>
        /// <param name="pCommandText">The stored procedure name or T-Sql command</param>
        /// <param name="pCommandParameters">An array of SqlParameters to be associated with the command or ' null' if no parameters are required</param>
        /// <param name="pConnectionOwnership">Indicates whether the connection parameter was provided by the caller, or created by Helper.SqlClient </param>
        /// <returns>SqlDataReader containing the results of the command</returns>
        private static SqlDataReader ExecuteReader(SqlConnection pSqlConnection, SqlTransaction pSqlTransaction, CommandType pCommandType, string pCommandText, SqlParameter[] pCommandParameters, SqlConnectionOwnership pConnectionOwnership)
        {
            if ((pSqlConnection == null))
                throw new ArgumentNullException("pSqlConnection");

            bool vMustCloseConnection = false;

            // Create a command and prepare it for execution
            SqlCommand vSqlCommand = new SqlCommand();

            try
            {
                // Create a reader
                SqlDataReader vDataReader = default(SqlDataReader);

                PrepareCommand(vSqlCommand, pSqlConnection, pSqlTransaction, pCommandType, pCommandText, pCommandParameters, ref vMustCloseConnection);
                vSqlCommand.CommandTimeout = 500;

                // Call ExecuteReader with the appropriate CommandBehavior
                if (pConnectionOwnership == SqlConnectionOwnership.External)
                    vDataReader = vSqlCommand.ExecuteReader();
                else
                    vDataReader = vSqlCommand.ExecuteReader(CommandBehavior.CloseConnection);

                // Detach the SqlParameters from the command object, so they can be used again
                bool vCanClear = true;

                foreach (SqlParameter commandParameter in vSqlCommand.Parameters)
                    if (commandParameter.Direction != ParameterDirection.Input)
                        vCanClear = false;

                if (vCanClear)
                    vSqlCommand.Parameters.Clear();

                return vDataReader;
            }
            catch
            {
                if (vMustCloseConnection)
                    pSqlConnection.Close();
                throw;
            }
        }

        // Execute a SqlCommand (that returns a resultset and takes no parameters) against the database specified in 
        // the connection string. 
        // e.g.:  
        // Dim dr As SqlDataReader = ExecuteReader(connString, CommandType.StoredProcedure, "GetOrders")
        // Parameters:
        // -connectionString - A valid connection string for a SqlConnection 
        // -commandType - The CommandType (stored procedure, text, etc.) 
        // -pCommandText - The stored procedure name or T-Sql command 
        // Returns: A SqlDataReader containing the resultset generated by the command 
        public static SqlDataReader ExecuteReader(string pConnectionString, CommandType pCommandType, string pCommandText)
        {
            // Pass through the call providing null for the set of SqlParameters
            return ExecuteReader(pConnectionString, pCommandType, pCommandText, (SqlParameter[])null);
        }
        // ExecuteReader

        // Execute a SqlCommand (that returns a resultset) against the database specified in the connection string 
        // using the provided parameters.
        // e.g.:  
        // Dim dr As SqlDataReader = ExecuteReader(connString, CommandType.StoredProcedure, "GetOrders", new SqlParameter("@prodid", 24))
        // Parameters:
        // -connectionString - A valid connection string for a SqlConnection 
        // -commandType - The CommandType (stored procedure, text, etc.) 
        // -pCommandText - The stored procedure name or T-Sql command 
        // -commandParameters - An array of SqlParamters used to execute the command 
        // Returns: A SqlDataReader containing the resultset generated by the command 
        public static SqlDataReader ExecuteReader(string connectionString, CommandType pCommandType, string pCommandText, params SqlParameter[] commandParameters)
        {
            if (string.IsNullOrEmpty(pConnectionString))
                throw new ArgumentNullException("pConnectionString");

            // Create & open a SqlConnection
            SqlConnection pSqlConnection = null;

            try
            {
                connection = new SqlConnection(connectionString);
                connection.Open();
                // Call the private overload that takes an internally owned connection in place of the connection string
                return ExecuteReader(connection, (SqlTransaction)null, commandType, pCommandText, commandParameters, SqlConnectionOwnership.Internal);
            }
            catch
            {
                // If we fail to return the SqlDatReader, we need to close the connection ourselves
                if ((connection != null))
                    connection.Dispose();
                throw;
            }
        }
        // ExecuteReader

        // Execute a stored procedure via a SqlCommand (that returns a resultset) against the database specified in 
        // the connection string using the provided parameter values.  This method will discover the parameters for the 
        // stored procedure, and assign the values based on parameter order.
        // This method provides no access to output parameters or the stored procedure' s return value parameter.
        // e.g.:  
        // Dim dr As SqlDataReader = ExecuteReader(connString, "GetOrders", 24, 36)
        // Parameters:
        // -connectionString - A valid connection string for a SqlConnection 
        // -pStoredProcedureName - the name of the stored procedure 
        // -pParameterValues - An array of objects to be assigned as the input values of the stored procedure 
        // Returns: A SqlDataReader containing the resultset generated by the command 
        public static SqlDataReader ExecuteReader(string connectionString, string pStoredProcedureName, params object[] pParameterValues)
        {
            if (string.IsNullOrEmpty(pConnectionString))
                throw new ArgumentNullException("pConnectionString");
            if (string.IsNullOrEmpty(pStoredProcedureName))
                throw new ArgumentNullException("pStoredProcedureName");

            SqlParameter[] commandParameters = null;

            // If we receive parameter values, we need to figure out where they go
            if ((pParameterValues != null) && pParameterValues.Length > 0)
            {
                // Pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                commandParameters = SqlClientParameterCache.GetSpParameterSet(connectionString, pStoredProcedureName);

                // Assign the provided values to these parameters based on parameter order
                AssignParameterValues(commandParameters, pParameterValues);

                // Call the overload that takes An array of SqlParameters
                return ExecuteReader(connectionString, CommandType.StoredProcedure, pStoredProcedureName, commandParameters);
                // Otherwise we can just call the SP without params
            }
            else
            {
                return ExecuteReader(connectionString, CommandType.StoredProcedure, pStoredProcedureName);
            }
        }
        // ExecuteReader

        // Execute a SqlCommand (that returns a resultset and takes no parameters) against the provided SqlConnection. 
        // e.g.:  
        // Dim dr As SqlDataReader = ExecuteReader(conn, CommandType.StoredProcedure, "GetOrders")
        // Parameters:
        // -connection - A valid SqlConnection 
        // -commandType - The CommandType (stored procedure, text, etc.) 
        // -pCommandText - The stored procedure name or T-Sql command 
        // Returns: A SqlDataReader containing the resultset generated by the command 
        public static SqlDataReader ExecuteReader(SqlConnection pSqlConnection, CommandType pCommandType, string pCommandText)
        {

            return ExecuteReader(connection, commandType, pCommandText, (SqlParameter[])null);

        }
        // ExecuteReader

        // Execute a SqlCommand (that returns a resultset) against the specified SqlConnection 
        // using the provided parameters.
        // e.g.:  
        // Dim dr As SqlDataReader = ExecuteReader(conn, CommandType.StoredProcedure, "GetOrders", new SqlParameter("@prodid", 24))
        // Parameters:
        // -connection - A valid SqlConnection 
        // -commandType - The CommandType (stored procedure, text, etc.) 
        // -pCommandText - The stored procedure name or T-Sql command 
        // -commandParameters - An array of SqlParamters used to execute the command 
        // Returns: A SqlDataReader containing the resultset generated by the command 
        public static SqlDataReader ExecuteReader(SqlConnection pSqlConnection, CommandType pCommandType, string pCommandText, params SqlParameter[] commandParameters)
        {
            // Pass through the call to private overload using a null transaction value
            return ExecuteReader(connection, (SqlTransaction)null, commandType, pCommandText, commandParameters, SqlConnectionOwnership.External);

        }
        // ExecuteReader

        // Execute a stored procedure via a SqlCommand (that returns a resultset) against the specified SqlConnection 
        // using the provided parameter values.  This method will discover the parameters for the 
        // stored procedure, and assign the values based on parameter order.
        // This method provides no access to output parameters or the stored procedure' s return value parameter.
        // e.g.:  
        // Dim dr As SqlDataReader = ExecuteReader(conn, "GetOrders", 24, 36)
        // Parameters:
        // -connection - A valid SqlConnection 
        // -pStoredProcedureName - the name of the stored procedure 
        // -pParameterValues - An array of objects to be assigned as the input values of the stored procedure 
        // Returns: A SqlDataReader containing the resultset generated by the command 
        public static SqlDataReader ExecuteReader(SqlConnection pSqlConnection, string pStoredProcedureName, params object[] pParameterValues)
        {
            if ((pSqlConnection == null))
                throw new ArgumentNullException("pSqlConnection");
            if (string.IsNullOrEmpty(pStoredProcedureName))
                throw new ArgumentNullException("pStoredProcedureName");

            SqlParameter[] commandParameters = null;
            // If we receive parameter values, we need to figure out where they go
            if ((pParameterValues != null) && pParameterValues.Length > 0)
            {
                commandParameters = SqlClientParameterCache.GetSpParameterSet(pSqlConnection, pStoredProcedureName);

                AssignParameterValues(commandParameters, pParameterValues);

                return ExecuteReader(pSqlConnection, CommandType.StoredProcedure, pStoredProcedureName, commandParameters);
                // Otherwise we can just call the SP without params
            }
            else
            {
                return ExecuteReader(pSqlConnection, CommandType.StoredProcedure, pStoredProcedureName);
            }

        }
        // ExecuteReader

        // Execute a SqlCommand (that returns a resultset and takes no parameters) against the provided SqlTransaction.
        // e.g.:  
        // Dim dr As SqlDataReader = ExecuteReader(trans, CommandType.StoredProcedure, "GetOrders")
        // Parameters:
        // -transaction - A valid SqlTransaction  
        // -commandType - The CommandType (stored procedure, text, etc.) 
        // -pCommandText - The stored procedure name or T-Sql command 
        // Returns: A SqlDataReader containing the resultset generated by the command 
        public static SqlDataReader ExecuteReader(SqlTransaction pSqlTransaction, CommandType pCommandType, string pCommandText)
        {
            // Pass through the call providing null for the set of SqlParameters
            return ExecuteReader(transaction, commandType, pCommandText, (SqlParameter[])null);
        }
        // ExecuteReader

        // Execute a SqlCommand (that returns a resultset) against the specified SqlTransaction
        // using the provided parameters.
        // e.g.:  
        // Dim dr As SqlDataReader = ExecuteReader(trans, CommandType.StoredProcedure, "GetOrders", new SqlParameter("@prodid", 24))
        // Parameters:
        // -transaction - A valid SqlTransaction 
        // -commandType - The CommandType (stored procedure, text, etc.)
        // -pCommandText - The stored procedure name or T-Sql command 
        // -commandParameters - An array of SqlParamters used to execute the command 
        // Returns: A SqlDataReader containing the resultset generated by the command 
        public static SqlDataReader ExecuteReader(SqlTransaction pSqlTransaction, CommandType pCommandType, string pCommandText, params SqlParameter[] commandParameters)
        {
            if (pSqlTransaction == null)
                throw new ArgumentNullException("pSqlTransaction");
            if ((pSqlTransaction != null) && (pSqlTransaction.Connection == null))
                throw new ArgumentException("The transaction was rollbacked or commited, please provide an open transaction.", "transaction");
            // Pass through to private overload, indicating that the connection is owned by the caller
            return ExecuteReader(transaction.Connection, transaction, commandType, pCommandText, commandParameters, SqlConnectionOwnership.Internal);
        }
        // ExecuteReader

        // Execute a stored procedure via a SqlCommand (that returns a resultset) against the specified SqlTransaction 
        // using the provided parameter values.  This method will discover the parameters for the 
        // stored procedure, and assign the values based on parameter order.
        // This method provides no access to output parameters or the stored procedure' s return value parameter.
        // e.g.:  
        // Dim dr As SqlDataReader = ExecuteReader(trans, "GetOrders", 24, 36)
        // Parameters:
        // -transaction - A valid SqlTransaction 
        // -pStoredProcedureName - the name of the stored procedure 
        // -pParameterValues - An array of objects to be assigned as the input values of the stored procedure
        // Returns: A SqlDataReader containing the resultset generated by the command
        public static SqlDataReader ExecuteReader(SqlTransaction pSqlTransaction, string pStoredProcedureName, params object[] pParameterValues)
        {
            if (pSqlTransaction == null)
                throw new ArgumentNullException("pSqlTransaction");
            if ((pSqlTransaction != null) && (pSqlTransaction.Connection == null))
                throw new ArgumentException("The transaction was rollbacked or commited, please provide an open transaction.", "transaction");
            if (string.IsNullOrEmpty(pStoredProcedureName))
                throw new ArgumentNullException("pStoredProcedureName");

            SqlParameter[] commandParameters = null;

            // If we receive parameter values, we need to figure out where they go
            if ((pParameterValues != null) && pParameterValues.Length > 0)
            {
                commandParameters = SqlClientParameterCache.GetSpParameterSet(transaction.Connection, pStoredProcedureName);

                AssignParameterValues(commandParameters, pParameterValues);

                return ExecuteReader(transaction, CommandType.StoredProcedure, pStoredProcedureName, commandParameters);
                // Otherwise we can just call the SP without params
            }
            else
            {
                return ExecuteReader(transaction, CommandType.StoredProcedure, pStoredProcedureName);
            }
        }
        // ExecuteReader

        #endregion

        #region "ExecuteScalar"

        // Execute a SqlCommand (that returns a 1x1 resultset and takes no parameters) against the database specified in 
        // the connection string. 
        // e.g.:  
        // Dim orderCount As Integer = CInt(ExecuteScalar(connString, CommandType.StoredProcedure, "GetOrderCount"))
        // Parameters:
        // -connectionString - A valid connection string for a SqlConnection 
        // -commandType - The CommandType (stored procedure, text, etc.) 
        // -pCommandText - The stored procedure name or T-Sql command 
        // Returns: An object containing the value in the 1x1 resultset generated by the command
        public static object ExecuteScalar(string connectionString, CommandType pCommandType, string pCommandText)
        {
            // Pass through the call providing null for the set of SqlParameters
            return ExecuteScalar(connectionString, commandType, pCommandText, (SqlParameter[])null);
        }
        // ExecuteScalar

        // Execute a SqlCommand (that returns a 1x1 resultset) against the database specified in the connection string 
        // using the provided parameters.
        // e.g.:  
        // Dim orderCount As Integer = Cint(ExecuteScalar(connString, CommandType.StoredProcedure, "GetOrderCount", new SqlParameter("@prodid", 24)))
        // Parameters:
        // -connectionString - A valid connection string for a SqlConnection 
        // -commandType - The CommandType (stored procedure, text, etc.) 
        // -pCommandText - The stored procedure name or T-Sql command 
        // -commandParameters - An array of SqlParamters used to execute the command 
        // Returns: An object containing the value in the 1x1 resultset generated by the command 
        public static object ExecuteScalar(string connectionString, CommandType pCommandType, string pCommandText, params SqlParameter[] commandParameters)
        {
            if (string.IsNullOrEmpty(pConnectionString))
                throw new ArgumentNullException("pConnectionString");
            // Create & open a SqlConnection, and dispose of it after we are done.
            SqlConnection pSqlConnection = null;
            try
            {
                connection = new SqlConnection(connectionString);
                connection.Open();

                // Call the overload that takes a connection in place of the connection string
                return ExecuteScalar(connection, commandType, pCommandText, commandParameters);
            }
            finally
            {
                if ((connection != null))
                    connection.Dispose();
            }
        }
        // ExecuteScalar

        // Execute a stored procedure via a SqlCommand (that returns a 1x1 resultset) against the database specified in 
        // the connection string using the provided parameter values.  This method will discover the parameters for the 
        // stored procedure, and assign the values based on parameter order.
        // This method provides no access to output parameters or the stored procedure' s return value parameter.
        // e.g.:  
        // Dim orderCount As Integer = CInt((connString, "GetOrderCount", 24, 36))
        // Parameters:
        // -connectionString - A valid connection string for a SqlConnection 
        // -pStoredProcedureName - the name of the stored procedure 
        // -pParameterValues - An array of objects to be assigned as the input values of the stored procedure 
        // Returns: An object containing the value in the 1x1 resultset generated by the command 
        public static object ExecuteScalar(string connectionString, string pStoredProcedureName, params object[] pParameterValues)
        {
            if (string.IsNullOrEmpty(pConnectionString))
                throw new ArgumentNullException("pConnectionString");
            if (string.IsNullOrEmpty(pStoredProcedureName))
                throw new ArgumentNullException("pStoredProcedureName");

            SqlParameter[] commandParameters = null;

            // If we receive parameter values, we need to figure out where they go
            if ((pParameterValues != null) && pParameterValues.Length > 0)
            {
                // Pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                commandParameters = SqlClientParameterCache.GetSpParameterSet(connectionString, pStoredProcedureName);

                // Assign the provided values to these parameters based on parameter order
                AssignParameterValues(commandParameters, pParameterValues);

                // Call the overload that takes An array of SqlParameters
                return ExecuteScalar(connectionString, CommandType.StoredProcedure, pStoredProcedureName, commandParameters);
                // Otherwise we can just call the SP without params
            }
            else
            {
                return ExecuteScalar(connectionString, CommandType.StoredProcedure, pStoredProcedureName);
            }
        }
        // ExecuteScalar

        // Execute a SqlCommand (that returns a 1x1 resultset and takes no parameters) against the provided SqlConnection. 
        // e.g.:  
        // Dim orderCount As Integer = CInt(ExecuteScalar(conn, CommandType.StoredProcedure, "GetOrderCount"))
        // Parameters:
        // -connection - A valid SqlConnection 
        // -commandType - The CommandType (stored procedure, text, etc.) 
        // -pCommandText - The stored procedure name or T-Sql command 
        // Returns: An object containing the value in the 1x1 resultset generated by the command 
        public static object ExecuteScalar(SqlConnection pSqlConnection, CommandType pCommandType, string pCommandText)
        {
            // Pass through the call providing null for the set of SqlParameters
            return ExecuteScalar(connection, commandType, pCommandText, (SqlParameter[])null);
        }
        // ExecuteScalar

        // Execute a SqlCommand (that returns a 1x1 resultset) against the specified SqlConnection 
        // using the provided parameters.
        // e.g.:  
        // Dim orderCount As Integer = CInt(ExecuteScalar(conn, CommandType.StoredProcedure, "GetOrderCount", new SqlParameter("@prodid", 24)))
        // Parameters:
        // -connection - A valid SqlConnection 
        // -commandType - The CommandType (stored procedure, text, etc.) 
        // -pCommandText - The stored procedure name or T-Sql command 
        // -commandParameters - An array of SqlParamters used to execute the command 
        // Returns: An object containing the value in the 1x1 resultset generated by the command 
        public static object ExecuteScalar(SqlConnection pSqlConnection, CommandType pCommandType, string pCommandText, params SqlParameter[] commandParameters)
        {

            if ((connection == null))
                throw new ArgumentNullException("connection");

            // Create a command and prepare it for execution
            SqlCommand vSqlCommand = new SqlCommand();
            object vReturnValue = null;
            bool vMustCloseConnection = false;

            PrepareCommand(vSqlCommand, connection, (SqlTransaction)null, commandType, pCommandText, commandParameters, ref vMustCloseConnection);
            vSqlCommand.CommandTimeout = 500;
            // Execute the command & return the results
            vReturnValue = vSqlCommand.ExecuteScalar();

            // Detach the SqlParameters from the command object, so they can be used again
            vSqlCommand.Parameters.Clear();

            if ((vMustCloseConnection))
                vSqlConnection.Close();

            return vReturnValue;

        }
        // ExecuteScalar

        // Execute a stored procedure via a SqlCommand (that returns a 1x1 resultset) against the specified SqlConnection 
        // using the provided parameter values.  This method will discover the parameters for the 
        // stored procedure, and assign the values based on parameter order.
        // This method provides no access to output parameters or the stored procedure' s return value parameter.
        // e.g.:  
        // Dim orderCount As Integer = CInt(ExecuteScalar(conn, "GetOrderCount", 24, 36))
        // Parameters:
        // -connection - A valid SqlConnection 
        // -pStoredProcedureName - the name of the stored procedure 
        // -pParameterValues - An array of objects to be assigned as the input values of the stored procedure 
        // Returns: An object containing the value in the 1x1 resultset generated by the command 
        public static object ExecuteScalar(SqlConnection pSqlConnection, string pStoredProcedureName, params object[] pParameterValues)
        {
            if ((connection == null))
                throw new ArgumentNullException("connection");
            if (string.IsNullOrEmpty(pStoredProcedureName))
                throw new ArgumentNullException("pStoredProcedureName");

            SqlParameter[] commandParameters = null;

            // If we receive parameter values, we need to figure out where they go
            if ((pParameterValues != null) && pParameterValues.Length > 0)
            {
                // Pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                commandParameters = SqlClientParameterCache.GetSpParameterSet(connection, pStoredProcedureName);

                // Assign the provided values to these parameters based on parameter order
                AssignParameterValues(commandParameters, pParameterValues);

                // Call the overload that takes An array of SqlParameters
                return ExecuteScalar(connection, CommandType.StoredProcedure, pStoredProcedureName, commandParameters);
                // Otherwise we can just call the SP without params
            }
            else
            {
                return ExecuteScalar(connection, CommandType.StoredProcedure, pStoredProcedureName);
            }

        }

        /// <summary>
        /// Execute a SqlCommand (that returns a 1x1 resultset and takes no parameters) against the provided SqlTransaction.
        /// e.g.:  
        /// 
        /// int orderCount = CInt(ExecuteScalar(trans, CommandType.StoredProcedure, "GetOrderCount"));
        /// </summary>
        /// <param name="pSqlTransaction">A valid SqlTransaction</param>
        /// <param name="pCommandType">The CommandType (stored procedure, text, etc.)</param>
        /// <param name="pCommandText">The stored procedure name or T-Sql command</param>
        /// <returns>An object containing the value in the 1x1 resultset generated by the command</returns>
        public static object ExecuteScalar(SqlTransaction pSqlTransaction, CommandType pCommandType, string pCommandText)
        {
            // Pass through the call providing null for the set of SqlParameters
            return ExecuteScalar(pSqlTransaction, pCommandType, pCommandText, null);
        }

        // Execute a SqlCommand (that returns a 1x1 resultset) against the specified SqlTransaction
        // using the provided parameters.
        // e.g.:  
        // Dim orderCount As Integer = CInt(ExecuteScalar(trans, CommandType.StoredProcedure, "GetOrderCount", new SqlParameter("@prodid", 24)))
        // Parameters:
        // -transaction - A valid SqlTransaction  
        // -commandType - The CommandType (stored procedure, text, etc.) 
        // -pCommandText - The stored procedure name or T-Sql command 
        // -commandParameters - An array of SqlParamters used to execute the command 
        // Returns: An object containing the value in the 1x1 resultset generated by the command 
        public static object ExecuteScalar(SqlTransaction pSqlTransaction, CommandType pCommandType, string pCommandText, params SqlParameter[] pCommandParameters)
        {
            if ((pSqlTransaction == null))
                throw new ArgumentNullException("pSqlTransaction");

            if (pSqlTransaction != null && pSqlTransaction.Connection == null)
                throw new ArgumentException("The transaction was rollbacked or commited, please provide an open transaction.", "pSqlTransaction");

            // Create a command and prepare it for execution
            SqlCommand vSqlCommand = new SqlCommand();
            object vReturnValue = null;
            bool vMustCloseConnection = false;

            PrepareCommand(vSqlCommand, pSqlTransaction.Connection, pSqlTransaction, pCommandType, pCommandText, pCommandParameters, ref vMustCloseConnection);
            vSqlCommand.CommandTimeout = 500;
            // Execute the command & return the results
            vReturnValue = vSqlCommand.ExecuteScalar();

            // Detach the SqlParameters from the command object, so they can be used again
            vSqlCommand.Parameters.Clear();

            return vReturnValue;
        }

        // Execute a stored procedure via a SqlCommand (that returns a 1x1 resultset) against the specified SqlTransaction 
        // using the provided parameter values.  This method will discover the parameters for the 
        // stored procedure, and assign the values based on parameter order.
        // This method provides no access to output parameters or the stored procedure' s return value parameter.
        // e.g.:  
        // Dim orderCount As Integer = CInt(ExecuteScalar(trans, "GetOrderCount", 24, 36))
        // Parameters:
        // -transaction - A valid SqlTransaction 
        // -pStoredProcedureName - the name of the stored procedure 
        // -pParameterValues - An array of objects to be assigned as the input values of the stored procedure 
        // Returns: An object containing the value in the 1x1 resultset generated by the command 
        public static object ExecuteScalar(SqlTransaction pSqlTransaction, string pStoredProcedureName, params object[] pParameterValues)
        {
            if (pSqlTransaction == null)
                throw new ArgumentNullException("pSqlTransaction");

            if ((pSqlTransaction != null) && (pSqlTransaction.Connection == null))
                throw new ArgumentException("The transaction was rollbacked or commited, please provide an open transaction.", "pSqlTransaction");

            if (string.IsNullOrEmpty(pStoredProcedureName))
                throw new ArgumentNullException("pStoredProcedureName");

            SqlParameter[] vCommandParameters = null;
            // If we receive parameter values, we need to figure out where they go
            if ((pParameterValues != null) && pParameterValues.Length > 0)
            {
                // Pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                vCommandParameters = SqlClientParameterCache.GetSpParameterSet(pSqlTransaction.Connection, pStoredProcedureName);

                // Assign the provided values to these parameters based on parameter order
                AssignParameterValues(vCommandParameters, pParameterValues);

                // Call the overload that takes An array of SqlParameters
                return ExecuteScalar(pSqlTransaction, CommandType.StoredProcedure, pStoredProcedureName, vCommandParameters);
                // Otherwise we can just call the SP without params
            }
            else
                return ExecuteScalar(pSqlTransaction, CommandType.StoredProcedure, pStoredProcedureName);
        }
        #endregion

        #region "FillDataset"
        /// <summary>
        /// Execute a SqlCommand (that returns a resultset and takes no parameters) against the database specified in 
        /// the connection string. 
        /// e.g.:  
        /// FillDataset (connString, CommandType.StoredProcedure, "GetOrders", ds, new String() {"orders"});
        /// </summary>
        /// <param name="pConnectionString">A valid connection string for a SqlConnection</param>
        /// <param name="pCommandType">The CommandType (stored procedure, text, etc.)</param>
        /// <param name="pCommandText">The stored procedure name or T-Sql command</param>
        /// <param name="pDataSet">A dataset wich will contain the resultset generated by the command</param>
        /// <param name="pTableNames">this array will be used to create table mappings allowing the DataTables to be referenced by a user defined name (probably the actual table name)</param>
        public static void FillDataset(string pConnectionString, CommandType pCommandType, string pCommandText, DataSet pDataSet, string[] pTableNames)
        {
            if (string.IsNullOrEmpty(pConnectionString))
                    throw new ArgumentNullException("pConnectionString");

            if (pDataSet == null)
                throw new ArgumentNullException("pDataSet");

            // Create & open a SqlConnection, and dispose of it after we are done
            SqlConnection vSqlConnection = null;

            try
            {
                vSqlConnection = new SqlConnection(pConnectionString);

                vSqlConnection.Open();

                // Call the overload that takes a connection in place of the connection string
                FillDataset(vSqlConnection, pCommandType, pCommandText, pDataSet, pTableNames);
            }
            finally
            {
                if (vSqlConnection != null)
                    vSqlConnection.Dispose();
            }
        }

        /// <summary>
        /// Execute a SqlCommand (that returns a resultset) against the database specified in the connection string 
        /// using the provided parameters.
        /// e.g.:  
        /// FillDataset (connString, CommandType.StoredProcedure, "GetOrders", ds, new String() = {"orders"}, new SqlParameter("@prodid", 24));
        /// </summary>
        /// <param name="pConnectionString">A valid connection string for a SqlConnection</param>
        /// <param name="pCommandType">The CommandType (stored procedure, text, etc.)</param>
        /// <param name="pCommandText">The stored procedure name or T-Sql command</param>
        /// <param name="pDataSet">A dataset wich will contain the resultset generated by the command</param>
        /// <param name="pTableNames">This array will be used to create table mappings allowing the DataTables to be referenced by a user defined name (probably the actual table name)</param>
        /// <param name="pCommandParameters">An array of SqlParamters used to execute the command</param>
        public static void FillDataset(string pConnectionString, CommandType pCommandType, string pCommandText, DataSet pDataSet, string[] pTableNames, params SqlParameter[] pCommandParameters)
        {
            if (string.IsNullOrEmpty(pConnectionString))
                throw new ArgumentNullException("pConnectionString");

            if (pDataSet == null)
                throw new ArgumentNullException("pDataSet");

            // Create & open a SqlConnection, and dispose of it after we are done
            SqlConnection vSqlConnection = null;

            try
            {
                vSqlConnection = new SqlConnection(pConnectionString);

                vSqlConnection.Open();

                // Call the overload that takes a connection in place of the connection string
                FillDataset(vSqlConnection, pCommandType, pCommandText, pDataSet, pTableNames, pCommandParameters);
            }
            finally
            {
                if ((vSqlConnection != null))
                    vSqlConnection.Dispose();
            }
        }

        /// <summary>
        /// Execute a stored procedure via a SqlCommand (that returns a resultset) against the database specified in 
        /// the connection string using the provided parameter values.  This method will query the database to discover the parameters for the 
        /// stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
        /// This method provides no access to output parameters or the stored procedure' s return value parameter.
        /// e.g.:  
        /// FillDataset (connString, CommandType.StoredProcedure, "GetOrders", ds, new String() {"orders"}, 24);
        /// </summary>
        /// <param name="pConnectionString">A valid connection string for a SqlConnection</param>
        /// <param name="pStoredProcedureName">The name of the stored procedure</param>
        /// <param name="pDataSet">A dataset wich will contain the resultset generated by the command</param>
        /// <param name="pTableNames">This array will be used to create table mappings allowing the DataTables to be referenced by a user defined name (probably the actual table name)</param>
        /// <param name="pParameterValues"> An array of objects to be assigned As the input values of the stored procedure</param>
        public static void FillDataset(string pConnectionString, string pStoredProcedureName, DataSet pDataSet, string[] pTableNames, params object[] pParameterValues)
        {
            if (string.IsNullOrEmpty(pConnectionString))
                throw new ArgumentNullException("pConnectionString");

            if ((pDataSet == null))
                throw new ArgumentNullException("pDataSet");

            // Create & open a SqlConnection, and dispose of it after we are done
            SqlConnection vSqlConnection = null;

            try
            {
                vSqlConnection = new SqlConnection(pConnectionString);

                vSqlConnection.Open();

                // Call the overload that takes a connection in place of the connection string
                FillDataset(vSqlConnection, pStoredProcedureName, pDataSet, pTableNames, pParameterValues);
            }
            finally
            {
                if (vSqlConnection != null)
                    vSqlConnection.Dispose();
            }
        }

        /// <summary>
        /// Execute a SqlCommand (that returns a resultset and takes no parameters) against the provided SqlConnection. 
        /// e.g.:  
        /// FillDataset (conn, CommandType.StoredProcedure, "GetOrders", ds, new String() {"orders"})
        /// </summary>
        /// <param name="pSqlConnection">A valid SqlConnection</param>
        /// <param name="pCommandType">The CommandType (stored procedure, text, etc.)</param>
        /// <param name="pCommandText">The stored procedure name or T-Sql command</param>
        /// <param name="pDataSet">A dataset wich will contain the resultset generated by the command</param>
        /// <param name="pTableNames">This array will be used to create table mappings allowing the DataTables to be referenced by a user defined name (probably the actual table name)</param>
        public static void FillDataset(SqlConnection pSqlConnection, CommandType pCommandType, string pCommandText, DataSet pDataSet, string[] pTableNames)
        {
            FillDataset(pSqlConnection, pCommandType, pCommandText, pDataSet, pTableNames, null);
        }

        /// <summary>
        /// Execute a SqlCommand (that returns a resultset) against the specified SqlConnection 
        /// using the provided parameters.
        /// e.g.:  
        /// FillDataset (conn, CommandType.StoredProcedure, "GetOrders", ds, new String() {"orders"}, new SqlParameter("@prodid", 24))
        /// </summary>
        /// <param name="pSqlConnection">A valid SqlConnection</param>
        /// <param name="pCommandType">The CommandType (stored procedure, text, etc.)</param>
        /// <param name="pCommandText">The stored procedure name or T-Sql command</param>
        /// <param name="pDataSet">A dataset wich will contain the resultset generated by the command</param>
        /// <param name="pTableNames">This array will be used to create table mappings allowing the DataTables to be referenced by a user defined name (probably the actual table name)</param>
        /// <param name="pCommandParameters">An array of SqlParamters used to execute the command</param>
        public static void FillDataset(SqlConnection pSqlConnection, CommandType pCommandType, string pCommandText, DataSet pDataSet, string[] pTableNames, params SqlParameter[] pCommandParameters)
        {
            FillDataset(pSqlConnection, null, pCommandType, pCommandText, pDataSet, pTableNames, pCommandParameters);
        }

        /// <summary>
        /// Execute a stored procedure via a SqlCommand (that returns a resultset) against the specified SqlConnection 
        /// using the provided parameter values.  This method will query the database to discover the parameters for the 
        /// stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
        /// This method provides no access to output parameters or the stored procedure' s return value parameter.
        /// e.g.:  
        /// FillDataset (conn, "GetOrders", ds, new string() {"orders"}, 24, 36);
        /// </summary>
        /// <param name="pSqlConnection">A valid SqlConnection</param>
        /// <param name="pStoredProcedureName">The name of the stored procedure</param>
        /// <param name="pDataSet">A dataset wich will contain the resultset generated by the command</param>
        /// <param name="pTableNames">This array will be used to create table mappings allowing the DataTables to be referenced by a user defined name (probably the actual table name)</param>
        /// <param name="pParameterValues">An array of objects to be assigned as the input values of the stored procedure</param>
        public static void FillDataset(SqlConnection pSqlConnection, string pStoredProcedureName, DataSet pDataSet, string[] pTableNames, params object[] pParameterValues)
        {
            if (pSqlConnection == null)
                throw new ArgumentNullException("pSqlConnection");

            if (pDataSet == null)
                throw new ArgumentNullException("pDataSet");

            if (string.IsNullOrEmpty(pStoredProcedureName))
                throw new ArgumentNullException("pStoredProcedureName");

            // If we receive parameter values, we need to figure out where they go

            if ((pParameterValues != null) && pParameterValues.Length > 0)
            {
                // Pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                SqlParameter[] vCommandParameters = SqlClientParameterCache.GetSpParameterSet(pSqlConnection, pStoredProcedureName);

                // Assign the provided values to these parameters based on parameter order
                AssignParameterValues(vCommandParameters, pParameterValues);

                // Call the overload that takes An array of SqlParameters
                FillDataset(pSqlConnection, CommandType.StoredProcedure, pStoredProcedureName, pDataSet, pTableNames, vCommandParameters);
                // Otherwise we can just call the SP without params
            }
            else
                FillDataset(pSqlConnection, CommandType.StoredProcedure, pStoredProcedureName, pDataSet, pTableNames);
        }

        /// <summary>
        /// Execute a SqlCommand (that returns a resultset and takes no parameters) against the provided SqlTransaction. 
        /// e.g.:  
        /// FillDataset (trans, CommandType.StoredProcedure, "GetOrders", ds, new string() {"orders"})
        /// </summary>
        /// <param name="pSqlTransaction">A valid SqlTransaction</param>
        /// <param name="pCommandType">The CommandType (stored procedure, text, etc.)</param>
        /// <param name="pCommandText">The stored procedure name or T-Sql command</param>
        /// <param name="pDataSet">A dataset wich will contain the resultset generated by the command</param>
        /// <param name="pTableNames">This array will be used to create table mappings allowing the DataTables to be referenced by a user defined name (probably the actual table name)</param>
        public static void FillDataset(SqlTransaction pSqlTransaction, CommandType pCommandType, string pCommandText, DataSet pDataSet, string[] pTableNames)
        {
            FillDataset(pSqlTransaction, pCommandType, pCommandText, pDataSet, pTableNames, null);
        }

        /// <summary>
        /// Execute a SqlCommand (that returns a resultset) against the specified SqlTransaction
        /// using the provided parameters.
        /// e.g.:  
        /// FillDataset(trans, CommandType.StoredProcedure, "GetOrders", ds, new string() {"orders"}, new SqlParameter("@prodid", 24));
        /// </summary>
        /// <param name="pSqlTransaction">A valid SqlTransaction</param>
        /// <param name="pCommandType">The CommandType (stored procedure, text, etc.)</param>
        /// <param name="pCommandText">The stored procedure name or T-Sql command</param>
        /// <param name="pDataSet">A dataset wich will contain the resultset generated by the command</param>
        /// <param name="pTableNames">This array will be used to create table mappings allowing the DataTables to be referenced by a user defined name (probably the actual table name)</param>
        /// <param name="pCommandParameters">An array of SqlParamters used to execute the command</param>
        public static void FillDataset(SqlTransaction pSqlTransaction, CommandType pCommandType, string pCommandText, DataSet pDataSet, string[] pTableNames, params SqlParameter[] pCommandParameters)
        {
            if (pSqlTransaction == null)
                throw new ArgumentNullException("pSqlTransaction");

            if (pSqlTransaction != null && pSqlTransaction.Connection == null)
                throw new ArgumentException("The transaction was rollbacked or commited, please provide an open transaction.", "pSqlTransaction");

            FillDataset(pSqlTransaction.Connection, pSqlTransaction, pCommandType, pCommandText, pDataSet, pTableNames, pCommandParameters);

        }

        // Execute a stored procedure via a SqlCommand (that returns a resultset) against the specified 
        // SqlTransaction using the provided parameter values.  This method will query the database to discover the parameters for the 
        // stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
        // This method provides no access to output parameters or the stored procedure' s return value parameter.
        // e.g.:  
        //   FillDataset(trans, "GetOrders", ds, new String(){"orders"}, 24, 36)
        // Parameters:
        // -transaction: A valid SqlTransaction
        // -pStoredProcedureName: the name of the stored procedure
        // -dataSet: A dataset wich will contain the resultset generated by the command
        // -pTableNames: this array will be used to create table mappings allowing the DataTables to be referenced
        //             by a user defined name (probably the actual table name)
        // -pParameterValues: An array of objects to be assigned as the input values of the stored procedure

        public static void FillDataset(SqlTransaction pSqlTransaction, string pStoredProcedureName, DataSet pDataSet, string[] pTableNames, params object[] pParameterValues)
        {
            if (pSqlTransaction == null)
                throw new ArgumentNullException("pSqlTransaction");
            if ((pSqlTransaction != null) && (pSqlTransaction.Connection == null))
                throw new ArgumentException("The transaction was rollbacked or commited, please provide an open transaction.", "transaction");
            if ((pDataSet == null))
                throw new ArgumentNullException("pDataSet");
            if (string.IsNullOrEmpty(pStoredProcedureName))
                throw new ArgumentNullException("pStoredProcedureName");

            // If we receive parameter values, we need to figure out where they go

            if ((pParameterValues != null) && pParameterValues.Length > 0)
            {
                // Pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                SqlParameter[] commandParameters = SqlClientParameterCache.GetSpParameterSet(transaction.Connection, pStoredProcedureName);

                // Assign the provided values to these parameters based on parameter order
                AssignParameterValues(commandParameters, pParameterValues);

                // Call the overload that takes An array of SqlParameters
                FillDataset(transaction, CommandType.StoredProcedure, pStoredProcedureName, dataSet, pTableNames, commandParameters);
                // Otherwise we can just call the SP without params
            }
            else
            {
                FillDataset(transaction, CommandType.StoredProcedure, pStoredProcedureName, dataSet, pTableNames);
            }
        }

        // Private helper method that execute a SqlCommand (that returns a resultset) against the specified SqlTransaction and SqlConnection
        // using the provided parameters.
        // e.g.:  
        //   FillDataset(conn, trans, CommandType.StoredProcedure, "GetOrders", ds, new String() {"orders"}, new SqlParameter("@prodid", 24))
        // Parameters:
        // -connection: A valid SqlConnection
        // -transaction: A valid SqlTransaction
        // -commandType: The CommandType (stored procedure, text, etc.)
        // -pCommandText: The stored procedure name or T-Sql command
        // -dataSet: A dataset wich will contain the resultset generated by the command
        // -pTableNames: this array will be used to create table mappings allowing the DataTables to be referenced
        //             by a user defined name (probably the actual table name)
        // -commandParameters: An array of SqlParamters used to execute the command

        private static void FillDataset(SqlConnection pSqlConnection, SqlTransaction pSqlTransaction, CommandType pCommandType, string pCommandText, DataSet pDataSet, string[] pTableNames, params SqlParameter[] commandParameters)
        {
            if ((connection == null))
                throw new ArgumentNullException("connection");
            if ((pDataSet == null))
                throw new ArgumentNullException("pDataSet");

            // Create a command and prepare it for execution
            SqlCommand command = new SqlCommand();

            bool vMustCloseConnection = false;
            PrepareCommand(command, connection, transaction, commandType, pCommandText, commandParameters, ref vMustCloseConnection);
            command.CommandTimeout = 500;
            // Create the DataAdapter & DataSet
            SqlDataAdapter dataAdapter = new SqlDataAdapter(command);

            try
            {
                // Add the table mappings specified by the user

                if ((pTableNames != null) && pTableNames.Length > 0)
                {
                    string tableName = "Table";
                    int index = 0;

                    for (index = 0; index <= pTableNames.Length - 1; index++)
                    {
                        if ((pTableNames(index) == null || pTableNames(index).Length == 0))
                            throw new ArgumentException("The pTableNames parameter must contain a list of tables, a value was provided as null or empty string.", "pTableNames");
                        dataAdapter.TableMappings.Add(tableName, pTableNames(index));
                        tableName = tableName + (index + 1).ToString();
                    }
                }

                // Fill the DataSet using default values for DataTable names, etc
                dataAdapter.Fill(dataSet);

                // Detach the SqlParameters from the command object, so they can be used again
                command.Parameters.Clear();
            }
            finally
            {
                if (((dataAdapter != null)))
                    dataAdapter.Dispose();
            }

            if ((vMustCloseConnection))
                vSqlConnection.Close();

        }
        #endregion

        #region "UpdateDataset"
        // Executes the respective command for each inserted, updated, or deleted row in the DataSet.
        // e.g.:  
        //   UpdateDataset(conn, insertCommand, deleteCommand, updateCommand, dataSet, "Order")
        // Parameters:
        // -insertCommand: A valid transact-Sql statement or stored procedure to insert new records into the data source
        // -deleteCommand: A valid transact-Sql statement or stored procedure to delete records from the data source
        // -updateCommand: A valid transact-Sql statement or stored procedure used to update records in the data source
        // -dataSet: the DataSet used to update the data source
        // -tableName: the DataTable used to update the data source

        public static void UpdateDataset(SqlCommand insertCommand, SqlCommand deleteCommand, SqlCommand updateCommand, DataSet pDataSet, string tableName)
        {
            if ((insertCommand == null))
                throw new ArgumentNullException("insertCommand");
            if ((deleteCommand == null))
                throw new ArgumentNullException("deleteCommand");
            if ((updateCommand == null))
                throw new ArgumentNullException("updateCommand");
            if ((pDataSet == null))
                throw new ArgumentNullException("pDataSet");
            if ((tableName == null || tableName.Length == 0))
                throw new ArgumentNullException("tableName");

            // Create a SqlDataAdapter, and dispose of it after we are done
            SqlDataAdapter dataAdapter = new SqlDataAdapter();
            try
            {
                // Set the data adapter commands
                dataAdapter.UpdateCommand = updateCommand;
                dataAdapter.InsertCommand = insertCommand;
                dataAdapter.DeleteCommand = deleteCommand;

                // Update the dataset changes in the data source
                dataAdapter.Update(dataSet, tableName);

                // Commit all the changes made to the DataSet
                dataSet.AcceptChanges();
            }
            finally
            {
                if (((dataAdapter != null)))
                    dataAdapter.Dispose();
            }
        }
        #endregion

        #region "CreateCommand"
        // Simplify the creation of a Sql command object by allowing
        // a stored procedure and optional parameters to be provided
        // e.g.:  
        // Dim command As SqlCommand = CreateCommand(conn, "AddCustomer", "CustomerID", "CustomerName")
        // Parameters:
        // -connection: A valid SqlConnection object
        // -pStoredProcedureName: the name of the stored procedure
        // -sourceColumns: An array of string to be assigned as the source columns of the stored procedure parameters
        // Returns:
        // A valid SqlCommand object
        public static SqlCommand CreateCommand(SqlConnection pSqlConnection, string pStoredProcedureName, params string[] sourceColumns)
        {

            if ((connection == null))
                throw new ArgumentNullException("connection");
            // Create a SqlCommand
            SqlCommand vSqlCommand = new SqlCommand(pStoredProcedureName, connection);
            vSqlCommand.CommandType = CommandType.StoredProcedure;

            // If we receive parameter values, we need to figure out where they go

            if ((sourceColumns != null) && sourceColumns.Length > 0)
            {
                // Pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                SqlParameter[] commandParameters = SqlClientParameterCache.GetSpParameterSet(connection, pStoredProcedureName);

                // Assign the provided source columns to these parameters based on parameter order
                int index = 0;
                for (index = 0; index <= sourceColumns.Length - 1; index++)
                {
                    commandParameters(index).SourceColumn = sourceColumns(index);
                }

                // Attach the discovered parameters to the SqlCommand object
                AttachParameters(vSqlCommand, commandParameters);
            }

            return vSqlCommand;
        }
        #endregion

        #region "ExecuteNonQueryTypedParams"
        // Execute a stored procedure via a SqlCommand (that returns no resultset) against the database specified in 
        // the connection string using the dataRow column values as the stored procedure' s parameters values.
        // This method will query the database to discover the parameters for the 
        // stored procedure (the first time each stored procedure is called), and assign the values based on row values.
        // Parameters:
        // -connectionString: A valid connection string for a SqlConnection
        // -pStoredProcedureName: the name of the stored procedure
        // -dataRow: The dataRow used to hold the stored procedure' s parameter values
        // Returns:
        // an int representing the number of rows affected by the command
        public static int ExecuteNonQueryTypedParams(string connectionString, string pStoredProcedureName, DataRow dataRow)
        {
            int vReturnValue = 0;
            if (string.IsNullOrEmpty(pConnectionString))
                throw new ArgumentNullException("pConnectionString");
            if (string.IsNullOrEmpty(pStoredProcedureName))
                throw new ArgumentNullException("pStoredProcedureName");

            // If the row has values, the store procedure parameters must be initialized

            if (((dataRow != null) && dataRow.ItemArray.Length > 0))
            {
                // Pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                SqlParameter[] commandParameters = SqlClientParameterCache.GetSpParameterSet(connectionString, pStoredProcedureName);

                // Set the parameters values
                AssignParameterValues(commandParameters, dataRow);

                vReturnValue = SqlClient.ExecuteNonquery(connectionString, CommandType.StoredProcedure, pStoredProcedureName, commandParameters);
            }
            else
            {
                vReturnValue = SqlClient.ExecuteNonquery(connectionString, CommandType.StoredProcedure, pStoredProcedureName);
            }
            return vReturnValue;
        }

        // Execute a stored procedure via a SqlCommand (that returns no resultset) against the specified SqlConnection 
        // using the dataRow column values as the stored procedure' s parameters values.  
        // This method will query the database to discover the parameters for the 
        // stored procedure (the first time each stored procedure is called), and assign the values based on row values.
        // Parameters:
        // -connection:A valid SqlConnection object
        // -pStoredProcedureName: the name of the stored procedure
        // -dataRow:The dataRow used to hold the stored procedure' s parameter values.
        // Returns:
        // an int representing the number of rows affected by the command
        public static int ExecuteNonQueryTypedParams(SqlConnection pSqlConnection, string pStoredProcedureName, DataRow dataRow)
        {
            int vReturnValue = 0;
            if ((connection == null))
                throw new ArgumentNullException("connection");
            if (string.IsNullOrEmpty(pStoredProcedureName))
                throw new ArgumentNullException("pStoredProcedureName");

            // If the row has values, the store procedure parameters must be initialized

            if (((dataRow != null) && dataRow.ItemArray.Length > 0))
            {
                // Pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                SqlParameter[] commandParameters = SqlClientParameterCache.GetSpParameterSet(connection, pStoredProcedureName);

                // Set the parameters values
                AssignParameterValues(commandParameters, dataRow);

                vReturnValue = SqlClient.ExecuteNonquery(connection, CommandType.StoredProcedure, pStoredProcedureName, commandParameters);
            }
            else
            {
                vReturnValue = SqlClient.ExecuteNonquery(connection, CommandType.StoredProcedure, pStoredProcedureName);
            }
            return vReturnValue;
        }

        // Execute a stored procedure via a SqlCommand (that returns no resultset) against the specified
        // SqlTransaction using the dataRow column values as the stored procedure' s parameters values.
        // This method will query the database to discover the parameters for the 
        // stored procedure (the first time each stored procedure is called), and assign the values based on row values.
        // Parameters:
        // -transaction:A valid SqlTransaction object
        // -pStoredProcedureName:the name of the stored procedure
        // -dataRow:The dataRow used to hold the stored procedure' s parameter values.
        // Returns:
        // an int representing the number of rows affected by the command
        public static int ExecuteNonQueryTypedParams(SqlTransaction pSqlTransaction, string pStoredProcedureName, DataRow dataRow)
        {
            int vReturnValue = 0;

            if (pSqlTransaction == null)
                throw new ArgumentNullException("pSqlTransaction");
            if ((pSqlTransaction != null) && (pSqlTransaction.Connection == null))
                throw new ArgumentException("The transaction was rollbacked or commited, please provide an open transaction.", "transaction");
            if (string.IsNullOrEmpty(pStoredProcedureName))
                throw new ArgumentNullException("pStoredProcedureName");

            // If the row has values, the store procedure parameters must be initialized

            if (((dataRow != null) && dataRow.ItemArray.Length > 0))
            {
                // Pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                SqlParameter[] commandParameters = SqlClientParameterCache.GetSpParameterSet(transaction.Connection, pStoredProcedureName);

                // Set the parameters values
                AssignParameterValues(commandParameters, dataRow);

                vReturnValue = SqlClient.ExecuteNonquery(transaction, CommandType.StoredProcedure, pStoredProcedureName, commandParameters);

            }
            else
            {
                vReturnValue = SqlClient.ExecuteNonquery(transaction, CommandType.StoredProcedure, pStoredProcedureName);
            }
            return vReturnValue;
        }
        #endregion

        #region "ExecuteDatasetTypedParams"
        // Execute a stored procedure via a SqlCommand (that returns a resultset) against the database specified in 
        // the connection string using the dataRow column values as the stored procedure' s parameters values.
        // This method will query the database to discover the parameters for the 
        // stored procedure (the first time each stored procedure is called), and assign the values based on row values.
        // Parameters:
        // -connectionString: A valid connection string for a SqlConnection
        // -pStoredProcedureName: the name of the stored procedure
        // -dataRow: The dataRow used to hold the stored procedure' s parameter values.
        // Returns:
        // a dataset containing the resultset generated by the command
        public static DataSet ExecuteDatasetTypedParams(string connectionString, string pStoredProcedureName, DataRow dataRow)
        {
            DataSet vReturnValue = default(DataSet);
            if (string.IsNullOrEmpty(pConnectionString))
                throw new ArgumentNullException("pConnectionString");
            if (string.IsNullOrEmpty(pStoredProcedureName))
                throw new ArgumentNullException("pStoredProcedureName");

            // If the row has values, the store procedure parameters must be initialized

            if (((dataRow != null) && dataRow.ItemArray.Length > 0))
            {
                // Pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                SqlParameter[] commandParameters = SqlClientParameterCache.GetSpParameterSet(connectionString, pStoredProcedureName);

                // Set the parameters values
                AssignParameterValues(commandParameters, dataRow);

                vReturnValue = SqlClient.ExecuteDataset(connectionString, CommandType.StoredProcedure, pStoredProcedureName, commandParameters);

            }
            else
            {
                vReturnValue = SqlClient.ExecuteDataset(connectionString, CommandType.StoredProcedure, pStoredProcedureName);
            }
            return vReturnValue;
        }

        // Execute a stored procedure via a SqlCommand (that returns a resultset) against the specified SqlConnection 
        // using the dataRow column values as the store procedure' s parameters values.
        // This method will query the database to discover the parameters for the 
        // stored procedure (the first time each stored procedure is called), and assign the values based on row values.
        // Parameters:
        // -connection: A valid SqlConnection object
        // -pStoredProcedureName: the name of the stored procedure
        // -dataRow: The dataRow used to hold the stored procedure' s parameter values.
        // Returns:
        // a dataset containing the resultset generated by the command
        public static DataSet ExecuteDatasetTypedParams(SqlConnection pSqlConnection, string pStoredProcedureName, DataRow dataRow)
        {
            DataSet vReturnValue = default(DataSet);

            if ((connection == null))
                throw new ArgumentNullException("connection");
            if (string.IsNullOrEmpty(pStoredProcedureName))
                throw new ArgumentNullException("pStoredProcedureName");

            // If the row has values, the store procedure parameters must be initialized

            if (((dataRow != null) && dataRow.ItemArray.Length > 0))
            {
                // Pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                SqlParameter[] commandParameters = SqlClientParameterCache.GetSpParameterSet(connection, pStoredProcedureName);

                // Set the parameters values
                AssignParameterValues(commandParameters, dataRow);

                vReturnValue = SqlClient.ExecuteDataset(connection, CommandType.StoredProcedure, pStoredProcedureName, commandParameters);

            }
            else
            {
                vReturnValue = SqlClient.ExecuteDataset(connection, CommandType.StoredProcedure, pStoredProcedureName);
            }
            return vReturnValue;
        }

        // Execute a stored procedure via a SqlCommand (that returns a resultset) against the specified SqlTransaction 
        // using the dataRow column values as the stored procedure' s parameters values.
        // This method will query the database to discover the parameters for the 
        // stored procedure (the first time each stored procedure is called), and assign the values based on row values.
        // Parameters:
        // -transaction: A valid SqlTransaction object
        // -pStoredProcedureName: the name of the stored procedure
        // -dataRow: The dataRow used to hold the stored procedure' s parameter values.
        // Returns:
        // a dataset containing the resultset generated by the command
        public static DataSet ExecuteDatasetTypedParams(SqlTransaction pSqlTransaction, string pStoredProcedureName, DataRow dataRow)
        {
            DataSet vReturnValue = default(DataSet);
            if (pSqlTransaction == null)
                throw new ArgumentNullException("pSqlTransaction");
            if ((pSqlTransaction != null) && (pSqlTransaction.Connection == null))
                throw new ArgumentException("The transaction was rollbacked or commited, please provide an open transaction.", "transaction");
            if (string.IsNullOrEmpty(pStoredProcedureName))
                throw new ArgumentNullException("pStoredProcedureName");

            // If the row has values, the store procedure parameters must be initialized

            if (((dataRow != null) && dataRow.ItemArray.Length > 0))
            {
                // Pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                SqlParameter[] commandParameters = SqlClientParameterCache.GetSpParameterSet(transaction.Connection, pStoredProcedureName);

                // Set the parameters values
                AssignParameterValues(commandParameters, dataRow);

                vReturnValue = SqlClient.ExecuteDataset(transaction, CommandType.StoredProcedure, pStoredProcedureName, commandParameters);

            }
            else
            {
                vReturnValue = SqlClient.ExecuteDataset(transaction, CommandType.StoredProcedure, pStoredProcedureName);
            }
            return vReturnValue;
        }
        #endregion

        #region "ExecuteReaderTypedParams"
        // Execute a stored procedure via a SqlCommand (that returns a resultset) against the database specified in 
        // the connection string using the dataRow column values as the stored procedure' s parameters values.
        // This method will query the database to discover the parameters for the 
        // stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
        // Parameters:
        // -connectionString: A valid connection string for a SqlConnection
        // -pStoredProcedureName: the name of the stored procedure
        // -dataRow: The dataRow used to hold the stored procedure' s parameter values.
        // Returns:
        // a SqlDataReader containing the resultset generated by the command
        public static SqlDataReader ExecuteReaderTypedParams(string connectionString, string pStoredProcedureName, DataRow dataRow)
        {
            SqlDataReader vReturnValue = default(SqlDataReader);
            if (string.IsNullOrEmpty(pConnectionString))
                throw new ArgumentNullException("pConnectionString");
            if (string.IsNullOrEmpty(pStoredProcedureName))
                throw new ArgumentNullException("pStoredProcedureName");

            // If the row has values, the store procedure parameters must be initialized

            if (((dataRow != null) && dataRow.ItemArray.Length > 0))
            {
                // Pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                SqlParameter[] commandParameters = SqlClientParameterCache.GetSpParameterSet(connectionString, pStoredProcedureName);

                // Set the parameters values
                AssignParameterValues(commandParameters, dataRow);

                vReturnValue = SqlClient.ExecuteReader(connectionString, CommandType.StoredProcedure, pStoredProcedureName, commandParameters);
            }
            else
            {
                vReturnValue = SqlClient.ExecuteReader(connectionString, CommandType.StoredProcedure, pStoredProcedureName);
            }
            return vReturnValue;
        }

        // Execute a stored procedure via a SqlCommand (that returns a resultset) against the specified SqlConnection 
        // using the dataRow column values as the stored procedure' s parameters values.
        // This method will query the database to discover the parameters for the 
        // stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
        // Parameters:
        // -connection: A valid SqlConnection object
        // -pStoredProcedureName: The name of the stored procedure
        // -dataRow: The dataRow used to hold the stored procedure' s parameter values.
        // Returns:
        // a SqlDataReader containing the resultset generated by the command
        public static SqlDataReader ExecuteReaderTypedParams(SqlConnection pSqlConnection, string pStoredProcedureName, DataRow dataRow)
        {
            SqlDataReader vReturnValue = default(SqlDataReader);
            if ((connection == null))
                throw new ArgumentNullException("connection");
            if (string.IsNullOrEmpty(pStoredProcedureName))
                throw new ArgumentNullException("pStoredProcedureName");

            // If the row has values, the store procedure parameters must be initialized

            if (((dataRow != null) && dataRow.ItemArray.Length > 0))
            {
                // Pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                SqlParameter[] commandParameters = SqlClientParameterCache.GetSpParameterSet(connection, pStoredProcedureName);

                // Set the parameters values
                AssignParameterValues(commandParameters, dataRow);

                vReturnValue = SqlClient.ExecuteReader(connection, CommandType.StoredProcedure, pStoredProcedureName, commandParameters);
            }
            else
            {
                vReturnValue = SqlClient.ExecuteReader(connection, CommandType.StoredProcedure, pStoredProcedureName);
            }
            return vReturnValue;
        }

        // Execute a stored procedure via a SqlCommand (that returns a resultset) against the specified SqlTransaction 
        // using the dataRow column values as the stored procedure' s parameters values.
        // This method will query the database to discover the parameters for the 
        // stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
        // Parameters:
        // -transaction: A valid SqlTransaction object
        // -pStoredProcedureName" The name of the stored procedure
        // -dataRow: The dataRow used to hold the stored procedure' s parameter values.
        // Returns:
        // a SqlDataReader containing the resultset generated by the command
        public static SqlDataReader ExecuteReaderTypedParams(SqlTransaction pSqlTransaction, string pStoredProcedureName, DataRow dataRow)
        {
            SqlDataReader vReturnValue = default(SqlDataReader);
            if (pSqlTransaction == null)
                throw new ArgumentNullException("pSqlTransaction");
            if ((pSqlTransaction != null) && (pSqlTransaction.Connection == null))
                throw new ArgumentException("The transaction was rollbacked or commited, please provide an open transaction.", "transaction");
            if (string.IsNullOrEmpty(pStoredProcedureName))
                throw new ArgumentNullException("pStoredProcedureName");

            // If the row has values, the store procedure parameters must be initialized

            if (((dataRow != null) && dataRow.ItemArray.Length > 0))
            {
                // Pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                SqlParameter[] commandParameters = SqlClientParameterCache.GetSpParameterSet(transaction.Connection, pStoredProcedureName);

                // Set the parameters values
                AssignParameterValues(commandParameters, dataRow);

                vReturnValue = SqlClient.ExecuteReader(transaction, CommandType.StoredProcedure, pStoredProcedureName, commandParameters);
            }
            else
            {
                vReturnValue = SqlClient.ExecuteReader(transaction, CommandType.StoredProcedure, pStoredProcedureName);
            }
            return vReturnValue;
        }
        #endregion

        #region "ExecuteScalarTypedParams"
        // Execute a stored procedure via a SqlCommand (that returns a 1x1 resultset) against the database specified in 
        // the connection string using the dataRow column values as the stored procedure' s parameters values.
        // This method will query the database to discover the parameters for the 
        // stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
        // Parameters:
        // -connectionString: A valid connection string for a SqlConnection
        // -pStoredProcedureName: The name of the stored procedure
        // -dataRow: The dataRow used to hold the stored procedure' s parameter values.
        // Returns:
        // An object containing the value in the 1x1 resultset generated by the command</returns>
        public static object ExecuteScalarTypedParams(string connectionString, string pStoredProcedureName, DataRow dataRow)
        {
            object vReturnValue = null;
            if (string.IsNullOrEmpty(pConnectionString))
                throw new ArgumentNullException("pConnectionString");
            if (string.IsNullOrEmpty(pStoredProcedureName))
                throw new ArgumentNullException("pStoredProcedureName");
            // If the row has values, the store procedure parameters must be initialized

            if (((dataRow != null) && dataRow.ItemArray.Length > 0))
            {
                // Pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                SqlParameter[] commandParameters = SqlClientParameterCache.GetSpParameterSet(connectionString, pStoredProcedureName);

                // Set the parameters values
                AssignParameterValues(commandParameters, dataRow);

                vReturnValue = SqlClient.ExecuteScalar(connectionString, CommandType.StoredProcedure, pStoredProcedureName, commandParameters);
            }
            else
            {
                vReturnValue = SqlClient.ExecuteScalar(connectionString, CommandType.StoredProcedure, pStoredProcedureName);
            }
            return vReturnValue;
        }

        // Execute a stored procedure via a SqlCommand (that returns a 1x1 resultset) against the specified SqlConnection 
        // using the dataRow column values as the stored procedure' s parameters values.
        // This method will query the database to discover the parameters for the 
        // stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
        // Parameters:
        // -connection: A valid SqlConnection object
        // -pStoredProcedureName: the name of the stored procedure
        // -dataRow: The dataRow used to hold the stored procedure' s parameter values.
        // Returns: 
        // an object containing the value in the 1x1 resultset generated by the command</returns>
        public static object ExecuteScalarTypedParams(SqlConnection pSqlConnection, string pStoredProcedureName, DataRow dataRow)
        {
            object vReturnValue = null;
            if ((connection == null))
                throw new ArgumentNullException("connection");
            if (string.IsNullOrEmpty(pStoredProcedureName))
                throw new ArgumentNullException("pStoredProcedureName");

            // If the row has values, the store procedure parameters must be initialized

            if (((dataRow != null) && dataRow.ItemArray.Length > 0))
            {
                // Pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                SqlParameter[] commandParameters = SqlClientParameterCache.GetSpParameterSet(connection, pStoredProcedureName);

                // Set the parameters values
                AssignParameterValues(commandParameters, dataRow);

                vReturnValue = SqlClient.ExecuteScalar(connection, CommandType.StoredProcedure, pStoredProcedureName, commandParameters);
            }
            else
            {
                vReturnValue = SqlClient.ExecuteScalar(connection, CommandType.StoredProcedure, pStoredProcedureName);
            }
            return vReturnValue;
        }

        // Execute a stored procedure via a SqlCommand (that returns a 1x1 resultset) against the specified SqlTransaction
        // using the dataRow column values as the stored procedure' s parameters values.
        // This method will query the database to discover the parameters for the 
        // stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
        // Parameters:
        // -transaction: A valid SqlTransaction object
        // -pStoredProcedureName: the name of the stored procedure
        // -dataRow: The dataRow used to hold the stored procedure' s parameter values.
        // Returns: 
        // an object containing the value in the 1x1 resultset generated by the command</returns>
        public static object ExecuteScalarTypedParams(SqlTransaction pSqlTransaction, string pStoredProcedureName, DataRow dataRow)
        {
            object vReturnValue = null;
            if (pSqlTransaction == null)
                throw new ArgumentNullException("pSqlTransaction");
            if ((pSqlTransaction != null) && (pSqlTransaction.Connection == null))
                throw new ArgumentException("The transaction was rollbacked or commited, please provide an open transaction.", "transaction");
            if (string.IsNullOrEmpty(pStoredProcedureName))
                throw new ArgumentNullException("pStoredProcedureName");

            // If the row has values, the store procedure parameters must be initialized

            if (((dataRow != null) && dataRow.ItemArray.Length > 0))
            {
                // Pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                SqlParameter[] commandParameters = SqlClientParameterCache.GetSpParameterSet(transaction.Connection, pStoredProcedureName);

                // Set the parameters values
                AssignParameterValues(commandParameters, dataRow);

                vReturnValue = SqlClient.ExecuteScalar(transaction, CommandType.StoredProcedure, pStoredProcedureName, commandParameters);
            }
            else
            {
                vReturnValue = SqlClient.ExecuteScalar(transaction, CommandType.StoredProcedure, pStoredProcedureName);
            }
            return vReturnValue;
        }
        #endregion

    }
    //SqlClient
    internal sealed class SqlClientParameterCache
    {
        #region "private methods, variables, and constructors"
        /// <summary>
        /// Since this class provides only static methods, make the default constructor private to prevent 
        /// instances from being created with "newSqlClientHelperParameterCache()".
        /// </summary>
        private SqlClientParameterCache()
        {
        }

        private static Hashtable vParamCache = Hashtable.Synchronized(new Hashtable());

        // 
        // Parameters:
        // - connectionString - 
        // - pStoredProcedureName - 
        // - includeReturnValueParameter - >
        // Returns: SqlParameter()
        /// <summary>
        /// Resolve at run time the appropriate set of SqlParameters for a stored procedure
        /// </summary>
        /// <param name="pSqlConnection">A valid connection string for a SqlConnection</param>
        /// <param name="pStoredProcedureName">The name of the stored procedure</param>
        /// <param name="pIncludeReturnValueParameter">Whether or not to include their return value parameter</param>
        /// <param name="pParameterValues"></param>
        /// <returns></returns>
        private static SqlParameter[] DiscoverSpParameterSet(SqlConnection pSqlConnection, string pStoredProcedureName, bool pIncludeReturnValueParameter, params object[] pParameterValues)
        {

            if (pSqlConnection == null)
                throw new ArgumentNullException("connection");

            if (string.IsNullOrEmpty(pStoredProcedureName))
                throw new ArgumentNullException("pStoredProcedureName");

            SqlCommand vSqlCommand = new SqlCommand(pStoredProcedureName, pSqlConnection);
            vSqlCommand.CommandType = CommandType.StoredProcedure;
            SqlParameter[] vDiscoveredParameters = null;

            pSqlConnection.Open();
            SqlCommandBuilder.DeriveParameters(vSqlCommand);
            pSqlConnection.Close();

            if (!pIncludeReturnValueParameter)
            {
                try
                {
                    vSqlCommand.Parameters.RemoveAt(0);
                }
                catch (Exception ex)
                {
                }
            }

            vDiscoveredParameters = new SqlParameter[vSqlCommand.Parameters.Count];
            vSqlCommand.Parameters.CopyTo(vDiscoveredParameters, 0);

            // Init the parameters with a DBNull value
            SqlParameter vDiscoveredParameter = default(SqlParameter);

            foreach (SqlParameter tmpDiscoveredParameter in vDiscoveredParameters)
            {
                vDiscoveredParameter.Value = DBNull.Value;
            }

            return vDiscoveredParameters;

        }

        /// <summary>
        /// Deep copy of cached SqlParameter array
        /// </summary>
        /// <param name="originalParameters"></param>
        /// <returns></returns>
        private static SqlParameter[] CloneParameters(SqlParameter[] pOriginalParameters)
        {
            int i = 0;
            int j = pOriginalParameters.Length - 1;

            SqlParameter[] vClonedParameters = new SqlParameter[j + 1];

            for (i = 0; i <= j; i++)
                vClonedParameters[i] = (SqlParameter)((ICloneable)pOriginalParameters[i]).Clone();

            return vClonedParameters;
        }
        #endregion

        #region "caching functions"

        // 
        // Parameters
        // -connectionString - A valid connection string for a SqlConnection 
        // -pCommandText - The stored procedure name or T-Sql command 
        // -commandParameters - An array of SqlParamters to be cached 
        /// <summary>
        /// Add parameter array to the cache
        /// </summary>
        /// <param name="pConnectionString"></param>
        /// <param name="pCommandText"></param>
        /// <param name="pCommandParameters"></param>
        public static void CacheParameterSet(string pConnectionString, string pCommandText, params SqlParameter[] pCommandParameters)
        {
            if (string.IsNullOrEmpty(pConnectionString))
                throw new ArgumentNullException("pConnectionString");

            if (string.IsNullOrEmpty(pCommandText))
                throw new ArgumentNullException("pCommandText");

            string hashKey = pConnectionString + ":" + pCommandText;

            paramCache[hashKey] = pCommandParameters;
        }
        // CacheParameterSet

        /// <summary>
        /// Retrieve a parameter array from the cache
        /// </summary>
        /// <param name="pConnectionString">A valid connection string for a SqlConnection</param>
        /// <param name="pCommandText">The stored procedure name or T-Sql command</param>
        /// <returns>An array of SqlParamters</returns>
        public static SqlParameter[] GetCachedParameterSet(string pConnectionString, string pCommandText)
        {
            if (string.IsNullOrEmpty(pConnectionString))
                throw new ArgumentNullException("pConnectionString");

            if (string.IsNullOrEmpty(pCommandText))
                throw new ArgumentNullException("pCommandText");

            string hashKey = pConnectionString + ":" + pCommandText;

            SqlParameter[] cachedParameters = (SqlParameter[])paramCache[hashKey];

            return cachedParameters == null ? null : CloneParameters(cachedParameters);
        }
        #endregion

        #region "Parameter Discovery Functions"
        // Retrieves the set of SqlParameters appropriate for the stored procedure.
        // This method will query the database for this information, and then store it in a cache for future requests.
        // Parameters:
        // -connectionString - A valid connection string for a SqlConnection 
        // -pStoredProcedureName - the name of the stored procedure 
        // Returns: An array of SqlParameters
        public static SqlParameter[] GetSpParameterSet(string connectionString, string pStoredProcedureName)
        {
            return GetSpParameterSet(connectionString, pStoredProcedureName, false);
        }
        // GetSpParameterSet 

        // Retrieves the set of SqlParameters appropriate for the stored procedure.
        // This method will query the database for this information, and then store it in a cache for future requests.
        // Parameters:
        // -connectionString - A valid connection string for a SqlConnection
        // -pStoredProcedureName - the name of the stored procedure 
        // -includeReturnValueParameter - a bool value indicating whether the return value parameter should be included in the results 
        // Returns: An array of SqlParameters 
        public static SqlParameter[] GetSpParameterSet(string pConnectionString, string pStoredProcedureName, bool pIncludeReturnValueParameter)
        {
            SqlParameter[] vReturnValue = null;

            if (string.IsNullOrEmpty(pConnectionString))
                throw new ArgumentNullException("pConnectionString");

            SqlConnection vSqlConnection = null;

            try
            {
                vSqlConnection = new SqlConnection(pConnectionString);
                vReturnValue = GetSpParameterSetInternal(vSqlConnection, pStoredProcedureName, pIncludeReturnValueParameter);
            }
            finally
            {
                if (vSqlConnection != null)
                    vSqlConnection.Dispose();
            }

            return vReturnValue;
        }

        /// <summary>
        /// Retrieves the set of SqlParameters appropriate for the stored procedure.
        /// This method will query the database for this information, and then store it in a cache for future requests.
        /// </summary>
        /// <param name="pSqlConnection">A valid SqlConnection object</param>
        /// <param name="pStoredProcedureName">A bool value indicating whether the return value parameter should be included in the results</param>
        /// <returns>An array of SqlParameters</returns>
        public static SqlParameter[] GetSpParameterSet(SqlConnection pSqlConnection, string pStoredProcedureName)
        {
            return GetSpParameterSet(pSqlConnection, pStoredProcedureName, false);
        }

        // Retrieves the set of SqlParameters appropriate for the stored procedure.
        // This method will query the database for this information, and then store it in a cache for future requests.
        // Parameters:
        // -connection - A valid SqlConnection object
        // -pStoredProcedureName - the name of the stored procedure 
        // -includeReturnValueParameter - a bool value indicating whether the return value parameter should be included in the results 
        // Returns: An array of SqlParameters 
        public static SqlParameter[] GetSpParameterSet(SqlConnection pSqlConnection, string pStoredProcedureName, bool includeReturnValueParameter)
        {
            SqlParameter[] vReturnValue = null;

            if ((pSqlConnection == null))
                throw new ArgumentNullException("connection");

            SqlConnection vClonedConnection = null;

            try
            {
                vClonedConnection = ((ICloneable)pSqlConnection).Clone;
                vReturnValue = GetSpParameterSetInternal(vClonedConnection, pStoredProcedureName, includeReturnValueParameter);
            }
            finally
            {
                if ((vClonedConnection != null))
                    vClonedConnection.Dispose();
            }

            return vReturnValue;
        }
        // GetSpParameterSet

        // Retrieves the set of SqlParameters appropriate for the stored procedure.
        // This method will query the database for this information, and then store it in a cache for future requests.
        // Parameters:
        // -pConnection - A valid SqlConnection object
        // -pStoredProcedureName - the name of the stored procedure 
        // -pIncludeReturnValueParameter - a bool value indicating whether the return value parameter should be included in the results 
        // Returns: An array of SqlParameters 
        private static SqlParameter[] GetSpParameterSetInternal(SqlConnection pConnection, string pStoredProcedureName, bool pIncludeReturnValueParameter)
        {
            if ((pConnection == null))
                throw new ArgumentNullException("connection");

            SqlParameter[] cachedParameters = null;
            string hashKey = null;

            if (string.IsNullOrEmpty(pStoredProcedureName))
                throw new ArgumentNullException("pStoredProcedureName");

            hashKey = pConnection.ConnectionString + ":" + pStoredProcedureName + (pIncludeReturnValueParameter == true ? ":include ReturnValue Parameter" : "").ToString();

            cachedParameters = (SqlParameter[])paramCache[hashKey];

            if ((cachedParameters == null))
            {
                SqlParameter[] spParameters = DiscoverSpParameterSet(pConnection, pStoredProcedureName, pIncludeReturnValueParameter);
                paramCache[hashKey] = spParameters;
                cachedParameters = spParameters;

            }

            return CloneParameters(cachedParameters);
        }
        #endregion
    }
}