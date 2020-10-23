using Microsoft.Extensions.Configuration;
using System;
using System.Data;
using System.Data.Common;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace GKodo.DbManager
{
    public abstract class SpRunner<EType, DbType> : IDisposable where EType : Enum where DbType : DbConnection, IDbConnection, new()
    {
        private readonly string _ConfigBase;
        private readonly DbConnection _Connection;
        private readonly IConfiguration _Configuration;

        private readonly BindingFlags _BindingFlags = BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance;

        public SpRunner(IConfiguration configuration)
        {
            _Configuration = configuration;
            _ConfigBase = $"{typeof(EType).Name}";

            string connectionString = _Configuration[$"{_ConfigBase}:ConnectionString"];
            DbConnection connection = (DbConnection)Activator.CreateInstance(typeof(DbType), new[] { connectionString });
            _Connection = connection;
        }

        public void Dispose()
        {
            if (_Connection.State == ConnectionState.Open) _Connection.Close();
            _Connection.Dispose();
        }

        ~SpRunner() => Dispose();

        private async Task<(DbCommand, string)> CreateCommandSp<TInput>(EType procedure, TInput param) where TInput : new()
        {
            DbCommand command = _Connection.CreateCommand();
            string configProcedure = $"{_ConfigBase}:Procedures:{procedure.ToString()}";
            string configReq = $"{configProcedure}:Request";

            command.CommandType = CommandType.StoredProcedure;
            command.CommandText = _Configuration[$"{configProcedure}:Name"] ?? procedure.ToString();

            PropertyInfo[] properties = param?.GetType().GetProperties(_BindingFlags);

            if (properties != null && properties.Length > 0) foreach (PropertyInfo prop in properties)
                {
                    if (prop.Name == null) continue;
                    string propDbName = _Configuration[$"{configReq}:{prop.Name}"] ?? prop.Name;
                    object paramValue = prop.GetValue(param, null);

                    DbParameter parameter = command.CreateParameter();

                    parameter.Value = paramValue ?? DBNull.Value;
                    parameter.ParameterName = propDbName;

                    command.Parameters.Add(parameter);
                }

            if (command.Connection.State == ConnectionState.Closed) await command.Connection.OpenAsync();

            return (command, configProcedure);
        }

        protected async Task<Tuple<TOutput, int>> ExecuteSp<TInput, TOutput>(EType procedure, TInput param) where TOutput : ITuple where TInput : new()
        {
            (DbCommand command, string configProcedure) = await CreateCommandSp(procedure, param);
            using DbDataReader dataReader = await command.ExecuteReaderAsync();
            (TOutput, int) result = (default, dataReader.RecordsAffected);

            Type resultType = typeof(TOutput);
            FieldInfo[] queries = resultType.GetFields();
            dynamic resultQry = Activator.CreateInstance(resultType);

            foreach (FieldInfo query in queries)
            {
                Type typeList = query.FieldType;
                dynamic list = Activator.CreateInstance(typeList);
                Type typeDto = typeList.GetProperty("Item").PropertyType;
                string configQry = $"{configProcedure}:Queries:{typeDto.Name}";

                while (await dataReader.ReadAsync())
                {
                    dynamic dto = Activator.CreateInstance(typeDto);
                    list.Add(dto);

                    foreach (PropertyInfo propDto in typeDto.GetProperties(_BindingFlags))
                    {
                        string propDbName = _Configuration[$"{configQry}:{propDto.Name}"] ?? propDto.Name;
                        object value = dataReader.GetValue(propDbName);

                        typeDto.GetProperty($"{propDto.Name}").SetValue(dto, value);
                    }
                }

                query.SetValue(resultQry as object, list);
                if (!await dataReader.NextResultAsync()) break;
                else continue;
            }

            result.Item1 = resultQry;

            await dataReader.CloseAsync();
            await dataReader.DisposeAsync();

            command.Dispose();

            return result.ToTuple();
        }

        protected async Task<int> ExecuteSp<TInput>(EType procedure, TInput param) where TInput : new()
        {
            (DbCommand command, _) = await CreateCommandSp(procedure, param);
            int result = await command.ExecuteNonQueryAsync();
            command.Dispose();

            return result;
        }
    }
}
