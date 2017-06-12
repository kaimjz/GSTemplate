using System.Data.Common;
using System.Data.SqlClient;
using Commons;

namespace DAL
{
    public class DBFactory
    {
        public DBFactory()
        {
        }

        /// <summary>
        /// 获取链接
        /// </summary>
        /// <returns></returns>
        public DbConnection GetInstance()
        {
            string connStr = GetConnString();
            if (ConfigHelper.GetValue("isEncrypt") == "True")//数据库是否加密
            {
                connStr = DESEncrypt.Decode(connStr);
            }

            DbConnection instance = new SqlConnection()
            {
                ConnectionString = connStr
            };
            return instance;
        }

        /// <summary>
        /// 获取链接字符串
        /// </summary>
        /// <returns></returns>
        public string GetConnString()
        {
            return ConfigHelper.GetValue("connectionString");
        }
    }
}