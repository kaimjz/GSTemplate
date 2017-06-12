using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;
using System.Text;
using Commons;
using DBUtility.Dapper;
using Models;

namespace DAL
{
    public class Base_DAL : IDisposable
    {
        public Base_DAL()
        {
        }

        #region 新增

        /// <summary>
        /// 实体对象添加
        /// </summary>
        /// <typeparam name="T">实体类型</typeparam>
        /// <param name="model">需要插入的实例</param>
        /// <returns>是否成功</returns>
        public Enums.TickLingEnum Insert<T>(T model)
        {
            try
            {
                Type typeFromHandle = typeof(T);
                string strTableName = typeFromHandle.Name;
                StringBuilder sbColumns = new StringBuilder();
                StringBuilder sbParameters = new StringBuilder();
                //将对象的所有属性和值转换成SQL参数形式
                foreach (PropertyInfo item in typeFromHandle.GetProperties())
                {
                    // add by zfj 2015-7-6 注释，待测试
                    if (item.IsDefined(typeof(PrimaryKeyAttribute), false) && (item.GetValue(model, null) == null)) //如果包含该主键,但主键为空时
                    {
                        item.SetValue(model, Guid.NewGuid().ToString(), null);
                    }
                    //if (item.Name.ToLower() == "createdate" && item.GetValue(model, null) == null)
                    //{
                    //    continue;
                    //}
                    //if (item.Name.ToLower() == "rowid")
                    //    continue;
                    if (item.IsDefined(typeof(ExtensionAttribute), false))
                    {
                        continue;
                    }
                    sbColumns.Append(item.Name + ",");
                    sbParameters.Append("@" + item.Name + ",");
                }

                string strSql = string.Format("insert into [" + strTableName + "]({0}) values({1})", sbColumns.ToString().Trim(','), sbParameters.ToString().Trim(','));
                using (DbConnection Connection = new DBFactory().GetInstance())
                {
                    Connection.Open();
                    return Connection.Execute(strSql, model, null, null, CommandType.Text) > 0 ? Enums.TickLingEnum.Success : Enums.TickLingEnum.Fail;
                }
            }
            catch (Exception ex)
            {
                return Enums.TickLingEnum.Abnormity;
            }
        }

        /// <summary>
        /// 实体对象添加,返回主键
        /// </summary>
        /// <typeparam name="T">实体类型</typeparam>
        /// <param name="model">需要插入的实例</param>
        /// <param name="pk">要返回的主键</param>
        /// <returns></returns>
        public Enums.TickLingEnum Insert<T>(T model, ref string pk)
        {
            try
            {
                Type typeFromHandle = typeof(T);
                string strTableName = typeFromHandle.Name;
                StringBuilder sbColumns = new StringBuilder();
                StringBuilder sbParameters = new StringBuilder();
                //将对象的所有属性和值转换成SQL参数形式
                foreach (PropertyInfo item in typeFromHandle.GetProperties())
                {
                    // add by zfj 2015-7-6 注释，待测试
                    if (item.IsDefined(typeof(PrimaryKeyAttribute), false) && (item.GetValue(model, null) == null)) //如果包含该主键,但主键为空时
                    {
                        pk = Guid.NewGuid().ToString();
                        item.SetValue(model, pk, null);
                    }
                    else
                    {
                        pk = item.GetValue(model, null).ToString();
                    }
                    //if (item.Name.ToLower() == "createdate" && item.GetValue(model, null) == null)
                    //{
                    //    continue;
                    //}
                    //if (item.Name.ToLower() == "rowid")
                    //    continue;
                    if (item.IsDefined(typeof(ExtensionAttribute), false))
                    {
                        continue;
                    }
                    sbColumns.Append(item.Name + ",");
                    sbParameters.Append("@" + item.Name + ",");
                }

                string strSql = string.Format("insert into [" + strTableName + "]({0}) values({1})", sbColumns.ToString().Trim(','), sbParameters.ToString().Trim(','));
                using (DbConnection Connection = new DBFactory().GetInstance())
                {
                    Connection.Open();
                    return Connection.Execute(strSql, model, null, null, CommandType.Text) > 0 ? Enums.TickLingEnum.Success : Enums.TickLingEnum.Fail;
                }
            }
            catch (Exception ex)
            {
                return Enums.TickLingEnum.Abnormity;
            }
        }

        /// <summary>
        /// 实体对象添加,返回主键
        /// </summary>
        /// <typeparam name="T">实体类型</typeparam>
        /// <param name="model">需要插入的实例</param>
        /// <param name="ht">需要判重字段</param>
        /// <returns></returns>
        public Enums.TickLingEnum Insert<T>(T model, Hashtable columnTable)
        {
            try
            {
                Type typeFromHandle = typeof(T);
                string strTableName = typeFromHandle.Name;
                StringBuilder sbColumns = new StringBuilder();
                StringBuilder sbParameters = new StringBuilder();

                Enums.TickLingEnum isExis = CheckColumnValueIsExist<T>(columnTable);
                if (isExis == Enums.TickLingEnum.Existence)
                {
                    return isExis;
                }

                //将对象的所有属性和值转换成SQL参数形式
                foreach (PropertyInfo item in typeFromHandle.GetProperties())
                {
                    // add by zfj 2015-7-6 注释，待测试
                    if (item.IsDefined(typeof(PrimaryKeyAttribute), false) && (item.GetValue(model, null) == null)) //如果包含该主键,但主键为空时
                    {
                        item.SetValue(model, Guid.NewGuid().ToString(), null);
                    }
                    //if (item.Name.ToLower() == "createdate" && item.GetValue(model, null) == null)
                    //{
                    //    continue;
                    //}
                    //if (item.Name.ToLower() == "rowid")
                    //    continue;
                    if (item.IsDefined(typeof(ExtensionAttribute), false))
                    {
                        continue;
                    }
                    sbColumns.Append(item.Name + ",");
                    sbParameters.Append("@" + item.Name + ",");
                }

                string strSql = string.Format("insert into [" + strTableName + "]({0}) values({1})", sbColumns.ToString().Trim(','), sbParameters.ToString().Trim(','));
                using (DbConnection Connection = new DBFactory().GetInstance())
                {
                    Connection.Open();
                    return Connection.Execute(strSql, model, null, null, CommandType.Text) > 0 ? Enums.TickLingEnum.Success : Enums.TickLingEnum.Fail;
                }
            }
            catch (Exception ex)
            {
                return Enums.TickLingEnum.Abnormity;
            }
        }

        /// <summary>
        /// 实体对象添加（事务）
        /// </summary>
        /// <typeparam name="T">实体类型</typeparam>
        /// <param name="obj">添加的实体对象</param>
        /// <param name="transaction">事务</param>
        /// <param name="Connection">数据库的连接</param>
        public Enums.TickLingEnum InsertTransaction<T>(T model, IDbTransaction transaction, DbConnection Connection)
        {
            Type typeFromHandle = typeof(T);
            string strTableName = typeFromHandle.Name;
            StringBuilder sbColumns = new StringBuilder();
            StringBuilder sbParameters = new StringBuilder();
            StringBuilder stringSqlTest = new StringBuilder();
            foreach (PropertyInfo item in typeFromHandle.GetProperties())
            {
                // add by zfj 2015-7-6 注释，待测试
                if (item.IsDefined(typeof(PrimaryKeyAttribute), false) && (item.GetValue(model, null) == null)) //如果包含该主键
                {
                    item.SetValue(model, Guid.NewGuid().ToString(), null);
                }
                if (item.Name.ToLower() == "createdate" && item.GetValue(model, null) == null)
                {
                    continue;
                }
                if (item.Name.ToLower() == "rowid")
                    continue;
                if (item.IsDefined(typeof(ExtensionAttribute), false))
                {
                    continue;
                }
                sbColumns.Append(item.Name + ",");
                sbParameters.Append("@" + item.Name + ",");

                //有问题时，调试脚本使用
                //if (item.GetValue(model, null) == null)
                //{
                //    stringSqlTest.Append("null,");
                //}
                //else
                //{
                //    stringSqlTest.Append("'" + item.GetValue(model, null) + "'" + ",");
                //}
            }

            //有问题时，调试脚本使用
            //string TEST_SQL = string.Format("insert into [" + strTableName + "]({0}) values({1})", sbColumns.ToString().Trim(','), stringSqlTest.ToString().Trim(','));
            string strSql = string.Format("insert into [" + strTableName + "]({0}) values({1})", sbColumns.ToString().Trim(','), sbParameters.ToString().Trim(','));

            return Connection.Execute(strSql, model, transaction, null, CommandType.Text) > 0 ? Enums.TickLingEnum.Success : Enums.TickLingEnum.Fail;
        }

        /// <summary>
        /// 通过事务批量新增
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="list"></param>
        /// <returns></returns>
        public Enums.TickLingEnum InsertModelsTransaction<T>(List<T> list)
        {
            Type typeFromHandle = typeof(T);
            string strTableName = typeFromHandle.Name;
            StringBuilder sbColumns = new StringBuilder();
            StringBuilder sbParameters = new StringBuilder();
            using (DbConnection Connection = new DBFactory().GetInstance())
            {
                Connection.Open();
                IDbTransaction transaction = Connection.BeginTransaction();
                try
                {
                    foreach (var item in list)
                    {
                        InsertTransaction(item, transaction, Connection);
                    }
                    transaction.Commit();
                    return Enums.TickLingEnum.Success;
                }
                catch (Exception ex)
                {
                    transaction.Rollback();
                    return Enums.TickLingEnum.Fail; ;
                }
            }
        }

        #endregion

        #region  删除

        /// <summary>
        /// 删除数据（根据主键）
        /// </summary>
        /// <typeparam name="T">实体类型</typeparam>
        /// <param name="id">主键id值</param>
        /// <returns>是否成功</returns>
        public Enums.TickLingEnum Delete<T>(string id)
        {
            try
            {
                if (string.IsNullOrEmpty(id))
                {
                    return Enums.TickLingEnum.Fail;
                }

                Type typeFromHandle = typeof(T);
                string strTableName = typeFromHandle.Name;
                StringBuilder sbWhere = new StringBuilder();
                var dynamicParameters = new DynamicParameters();
                var flag = false;//标识是否找到主键
                foreach (PropertyInfo item in typeFromHandle.GetProperties())
                {
                    if (item.IsDefined(typeof(PrimaryKeyAttribute), false)) //如果包含该主键
                    {
                        //获取主键条件
                        sbWhere.Append(item.Name + "=@" + item.Name);
                        dynamicParameters.Add(item.Name, id);
                        flag = true;
                    }
                }
                if (!flag)//未获取到了主键
                {
                    return Enums.TickLingEnum.Fail;//没有获取到主键
                }
                string strSql = string.Format("delete from [" + strTableName + "] where {0}", sbWhere);

                using (DbConnection Connection = new DBFactory().GetInstance())
                {
                    Connection.Open();
                    return Connection.Execute(strSql, dynamicParameters, null, null, CommandType.Text) > 0 ? Enums.TickLingEnum.Success : Enums.TickLingEnum.Fail;
                }
            }
            catch (Exception ex)
            {
                return Enums.TickLingEnum.Abnormity;
            }
        }

        /// <summary>
        /// 删除数据（根据条件）
        /// </summary>
        /// <typeparam name="T">实体类型</typeparam>
        /// <param name="Where">删除条件</param>
        /// <returns></returns>
        public Enums.TickLingEnum DeleteWhere<T>(string Where)
        {
            try
            {
                if (string.IsNullOrEmpty(Where))
                {
                    return Enums.TickLingEnum.Fail;
                }
                Type typeFromHandle = typeof(T);
                string strTableName = typeFromHandle.Name;
                string strSql = string.Format("delete from [" + strTableName + "] where {0}", Where);
                using (DbConnection Connection = new DBFactory().GetInstance())
                {
                    Connection.Open();
                    return Connection.Execute(strSql, null, null, null, CommandType.Text) > 0 ? Enums.TickLingEnum.Success : Enums.TickLingEnum.Fail;
                }
            }
            catch (Exception ex)
            {
                return Enums.TickLingEnum.Abnormity;
            }
        }

        /// <summary>
        /// 删除数据（事务）
        /// </summary>
        /// <typeparam name="T">实体类型</typeparam>
        /// <param name="where">拼接的where条件字符串</param>
        /// <param name="transaction">事务</param>
        /// <param name="Connection">数据库的连接</param>
        public Enums.TickLingEnum DeleteTransaction<T>(string where, IDbTransaction transaction, DbConnection Connection)
        {
            try
            {
                if (string.IsNullOrEmpty(where))
                {
                    return Enums.TickLingEnum.Fail;
                }
                Type typeFromHandle = typeof(T);
                string strTableName = typeFromHandle.Name;
                string strSql = string.Format("delete from [" + strTableName + "] where {0}", where);
                return Connection.Execute(strSql, null, transaction, null, CommandType.Text) > 0 ? Enums.TickLingEnum.Success : Enums.TickLingEnum.Fail;
            }
            catch (Exception ex)
            {
                return Enums.TickLingEnum.Abnormity;
            }
        }

        /// <summary>
        /// 批量删除(事务)
        /// </summary>
        /// <typeparam name="T">实体类型</typeparam>
        /// <param name="ids">主键集合</param>
        /// <returns></returns>
        public Enums.TickLingEnum DeleteModelsTransaction<T>(string[] ids)
        {
            using (DbConnection Connection = new DBFactory().GetInstance())
            {
                Connection.Open();
                IDbTransaction transaction = Connection.BeginTransaction();
                try
                {
                    Type typeFromHandle = typeof(T);
                    string strTableName = typeFromHandle.Name;
                    string PrimaryKeyName = string.Empty;
                    List<PropertyInfo> list = typeFromHandle.GetProperties().Where(p => p.IsDefined(typeof(PrimaryKeyAttribute), false) == true).ToList();
                    if (list.Count == 1)
                    {
                        PrimaryKeyName = list[0].Name;
                        foreach (var item in ids)
                        {
                            DeleteTransaction<T>(string.Format(" {0} = '{1}' ", PrimaryKeyName, item), transaction, Connection);
                        }
                        transaction.Commit();
                        return Enums.TickLingEnum.Success;
                    }
                    else
                    {
                        return Enums.TickLingEnum.Fail;
                    }
                }
                catch (Exception ex)
                {
                    transaction.Rollback();
                    return Enums.TickLingEnum.Abnormity;
                }
            }
        }

        #endregion

        #region 修改

        /// <summary>
        /// 修改数据(根据主键)
        /// </summary>
        /// <typeparam name="T">实体类型</typeparam>
        /// <param name="updateKeyValuePair">要修改字段的键值对</param>
        /// <param name="id">主键值</param>
        /// <returns>是否成功</returns>
        public Enums.TickLingEnum UpdateMultiColumnByID<T>(Hashtable updateKeyValuePair, string id, IDbTransaction transaction = null, DbConnection sqlConnection = null)
        {
            if (string.IsNullOrEmpty(id))
            {
                return Enums.TickLingEnum.Fail;
            }
            Type typeFromHandle = typeof(T);
            string strTableName = typeFromHandle.Name;
            StringBuilder sbColumns = new StringBuilder();
            StringBuilder sbwhere = new StringBuilder();
            var parems = new DynamicParameters();

            //捕获主键
            foreach (PropertyInfo item in typeFromHandle.GetProperties())
            {
                if (item.IsDefined(typeof(PrimaryKeyAttribute), false))
                {
                    sbwhere.AppendFormat(" {0} = @{1} ", item.Name, item.Name);
                    parems.Add("@" + item.Name, id); //添加主键参数
                }
            }
            //生成需要修改的字段和参数
            foreach (DictionaryEntry item in updateKeyValuePair)
            {
                if (item.Value != null)
                {
                    sbColumns.AppendFormat(" {0} = @{1} ,", item.Key, item.Key);
                    parems.Add("@" + item.Key, item.Value.ToString().Trim());
                }
                else
                {
                    sbColumns.Append(item.Key + "=null,");
                }
            }

            sbColumns.Remove(sbColumns.Length - 1, 1);

            string strSql = string.Format("update [" + strTableName + "] set {0} where {1}", sbColumns, sbwhere);
            using (DbConnection Connection = new DBFactory().GetInstance())
            {
                Connection.Open();
                if (transaction == null)
                {
                    //普通字段修改
                    return Connection.Execute(strSql, parems, null, null, CommandType.Text) > 0 ? Enums.TickLingEnum.Success : Enums.TickLingEnum.Fail;
                }
                else
                {
                    //执行事务
                    return sqlConnection.Execute(strSql, parems, transaction, null, CommandType.Text) > 0 ? Enums.TickLingEnum.Success : Enums.TickLingEnum.Fail;
                }
            }
        }

        /// <summary>
        /// 修改数据(根据条件)
        /// </summary>
        /// <typeparam name="T">实体类型</typeparam>
        /// <param name="updateKeyValuePair">要修改字段的键值对</param>
        /// <param name="where">修改条件</param>
        /// <returns>是否成功</returns>
        public Enums.TickLingEnum UpdateMultiColumnByWhere<T>(Hashtable updateKeyValuePair, string where, IDbTransaction transaction = null, DbConnection sqlConnection = null)
        {
            try
            {
                if (string.IsNullOrEmpty(where))
                {
                    return Enums.TickLingEnum.Fail;
                }
                Type typeFromHandle = typeof(T);
                string strTableName = typeFromHandle.Name;
                StringBuilder sbColumns = new StringBuilder();
                var parems = new DynamicParameters();

                //生成需要修改的字段和参数
                foreach (DictionaryEntry item in updateKeyValuePair)
                {
                    if (item.Value != null)
                    {
                        sbColumns.Append(item.Key + "=@" + item.Key + ",");
                        parems.Add("@" + item.Key, item.Value.ToString().Trim());
                    }
                    else
                    {
                        sbColumns.Append(item.Key + "=null,");
                    }
                }

                sbColumns.Remove(sbColumns.Length - 1, 1);

                string strSql = string.Format("update [" + strTableName + "] set {0} where {1}", sbColumns, where);
                using (DbConnection Connection = new DBFactory().GetInstance())
                {
                    Connection.Open();
                    if (transaction == null)
                    {
                        //普通字段修改
                        return Connection.Execute(strSql, parems, null, null, CommandType.Text) > 0 ? Enums.TickLingEnum.Success : Enums.TickLingEnum.Fail;
                    }
                    else
                    {
                        //执行事务
                        return sqlConnection.Execute(strSql, parems, transaction, null, CommandType.Text) > 0 ? Enums.TickLingEnum.Success : Enums.TickLingEnum.Fail;
                    }
                }
            }
            catch (Exception)
            {
                return Enums.TickLingEnum.Abnormity;
            }
        }

        #endregion

        #region 查询

        /// <summary>
        /// 判断重复(根据条件)
        /// </summary>
        /// <typeparam name="T">实体类型</typeparam>
        /// <param name="columnTable">条件集合</param>
        /// <returns></returns>
        public Enums.TickLingEnum CheckColumnValueIsExist<T>(Hashtable columnTable)
        {
            try
            {
                Type typeFromHandle = typeof(T);
                string strTableName = typeFromHandle.Name;
                var parems = new DynamicParameters();
                StringBuilder sbwhere = new StringBuilder();

                if (columnTable.Count > 0)
                {
                    sbwhere.Append(" where ");
                    foreach (DictionaryEntry item in columnTable)
                    {
                        sbwhere.AppendFormat(" {0} = @{1} and", item.Key, item.Key);
                        parems.Add("@" + item.Key, item.Value.ToString().Trim()); //添加主键参数
                    }
                    sbwhere.Remove(sbwhere.Length - 3, 3);
                }

                string strSql = string.Format("select count(*) from [" + strTableName + "] {0}", sbwhere);
                using (DbConnection Connection = new DBFactory().GetInstance())
                {
                    Connection.Open();
                    return Convert.ToInt32(Connection.Query<int>(strSql, parems, null, false, null, CommandType.Text).ToList()[0]) > 0 ? Enums.TickLingEnum.Existence : Enums.TickLingEnum.NonExistence;
                }
            }
            catch (Exception ex)
            {
                return Enums.TickLingEnum.Abnormity;
            }
        }

        /// <summary>
        /// 判断重复(根据主键)
        /// </summary>
        /// <typeparam name="T">实体类型</typeparam>
        /// <param name="id">主键</param>
        /// <returns>是否存在</returns>
        public Enums.TickLingEnum ExistsByID<T>(string id)
        {
            try
            {
                Type typeFromHandle = typeof(T);
                string strTableName = typeFromHandle.Name;
                var parems = new DynamicParameters();
                string strwhere = string.Empty;
                //捕获主键
                foreach (PropertyInfo item in typeFromHandle.GetProperties())
                {
                    if (item.IsDefined(typeof(PrimaryKeyAttribute), false) && !string.IsNullOrEmpty(id))
                    {
                        strwhere = string.Format("where {0} = '{1}'", item.Name, id);
                        parems.Add("@" + item.Name, id); //添加主键参数
                    }
                }
                string strSql = string.Format("select count(*) from [" + strTableName + "] {0}", strwhere);
                using (DbConnection Connection = new DBFactory().GetInstance())
                {
                    Connection.Open();
                    return Convert.ToInt32(Connection.Query<int>(strSql, parems, null, false, null, CommandType.Text).ToList()[0]) > 0 ? Enums.TickLingEnum.Existence : Enums.TickLingEnum.NonExistence;
                }
            }
            catch (Exception ex)
            {
                return Enums.TickLingEnum.Abnormity;
            }
        }

        /// <summary>
        /// 查询数据(返回单值)
        /// </summary>
        /// <param name="sql">SQL 语句</param>
        /// <returns>查询的值</returns>
        public object SelectExecuteScalar(string sql)
        {
            object obj = null;
            using (SqlConnection conn = (SqlConnection)new DBFactory().GetInstance())
            using (DbCommand cmd = new SqlCommand(sql, conn))
            {
                conn.Open();
                cmd.CommandType = CommandType.Text;
                obj = cmd.ExecuteScalar();
            }
            return obj;
        }

        /// <summary>
        /// 查询数据(集合)
        /// </summary>
        /// <typeparam name="T">实体类型</typeparam>
        /// <param name="sql">SQL 语句</param>
        /// <returns>返回结果集</returns>
        public List<T> SelectListBySql<T>(string sql)
        {
            using (DbConnection Connection = new DBFactory().GetInstance())
            {
                Connection.Open();
                return SqlMapper.Query<T>(Connection, sql, null, null, true, null, CommandType.Text).ToList();
            }
        }

        /// <summary>
        /// 执行语句(返回影响行数)
        /// </summary>
        /// <param name="sql">SQL 语句</param>
        /// <returns>受影响行数</returns>
        public int SelectExecuteNonQuery(string sql)
        {
            int affectRowNum = 0;
            using (SqlConnection conn = (SqlConnection)new DBFactory().GetInstance())
            using (DbCommand cmd = new SqlCommand(sql, conn))
            {
                conn.Open();
                cmd.CommandType = CommandType.Text;
                affectRowNum = cmd.ExecuteNonQuery();
            }
            return affectRowNum;
        }

        /// <summary>
        /// 查询数据(实体)
        /// </summary>
        /// <typeparam name="T">实体类型</typeparam>
        /// <param name="where">拼接的where条件字符串</param>
        /// <returns>返回结果集</returns>
        public T SelectEntity<T>(string where)
        {
            try
            {
                Type typeFromHandle = typeof(T);
                string strTableName = typeFromHandle.Name;
                if (!string.IsNullOrEmpty(where))
                {
                    where = " where " + where;
                }
                string sql = string.Format("select * from [" + strTableName + "] {0}", where);

                using (DbConnection Connection = new DBFactory().GetInstance())
                {
                    Connection.Open();
                    return SqlMapper.Query<T>(Connection, sql, null, null, true, null, CommandType.Text).FirstOrDefault<T>();
                }
            }
            catch (Exception ex)
            {
                return default(T);
            }
        }

        /// <summary>
        /// 查询数据(集合)
        /// </summary>
        /// <typeparam name="T">实体类型</typeparam>
        /// <param name="where">拼接的查询条件字符串</param>
        /// <param name="topNumber">返回数据条数</param>
        /// <returns>返回结果集</returns>
        public List<T> Select<T>(string where, string orderby = "", int topNumber = -1)
        {
            try
            {
                Type typeFromHandle = typeof(T);
                string strTableName = typeFromHandle.Name;
                string sql = string.Empty;
                if (!string.IsNullOrEmpty(where))
                {
                    where = "  where " + where;
                }

                if (topNumber != -1)
                {
                    sql = string.Format("select top {0} * from [{1}] {2} {3}", topNumber, strTableName, where, orderby);
                }
                else
                {
                    sql = string.Format("select * from [{0}] {1} ", strTableName, where, orderby);
                }

                using (DbConnection Connection = new DBFactory().GetInstance())
                {
                    Connection.Open();
                    return SqlMapper.Query<T>(Connection, sql, null, null, true, null, CommandType.Text).ToList();
                }
            }
            catch (Exception ex)
            {
                return null;
            }
        }

        /// <summary>
        /// 查询数据(DataSet)
        /// </summary>
        /// <param name="SQLString">查询语句</param>
        /// <returns>DataSet</returns>
        public DataSet Select(string SQLString)
        {
            using (SqlConnection Connection = new SqlConnection(new DBFactory().GetConnString()))
            {
                DataSet ds = new DataSet();
                try
                {
                    Connection.Open();
                    SqlDataAdapter command = new SqlDataAdapter(SQLString, Connection);
                    command.Fill(ds, "ds");
                }
                catch (SqlException ex)
                {
                    throw new Exception(ex.Message);
                }
                return ds;
            }
        }

        /// <summary>
        /// IEnumerable转DataTable
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="collection">需要转化的集合</param>
        /// <returns></returns>
        public static DataTable ToDataTable<T>(IEnumerable<T> collection)
        {
            var props = typeof(T).GetProperties();
            var dt = new DataTable();

            dt.Columns.AddRange(props.Select(p => new DataColumn(p.Name)).ToArray());
            if (collection.Count() > 0)
            {
                for (int i = 0; i < collection.Count(); i++)
                {
                    ArrayList tempList = new ArrayList();
                    foreach (PropertyInfo pi in props)
                    {
                        object obj = pi.GetValue(collection.ElementAt(i), null);
                        tempList.Add(obj);
                    }
                    object[] array = tempList.ToArray();
                    dt.LoadDataRow(array, true);
                }
            }
            return dt;
        }

        #endregion

        #region 存储过程

        /// <summary>
        /// 根据输入参数和输出参数的存储过程
        /// </summary>
        /// <typeparam name="T">实体类型</typeparam>
        /// <param name="storedProcedureName">存储过程名称</param>
        /// <param name="normalParas">普通参数 键值对</param>
        /// <param name="outPutParas">输出参数 键值对</param>
        /// <returns>查询结果泛型集合</returns>
        public List<T> SelectByStoredProcedure<T>(string storedProcedureName, Hashtable normalParms, ref Hashtable outPutParms)
        {
            try
            {
                using (DbConnection Connection = new DBFactory().GetInstance())
                {
                    var parems = new DynamicParameters();
                    //添加普通参数
                    if (normalParms != null)
                    {
                        foreach (DictionaryEntry item in normalParms)
                        {
                            parems.Add("@" + item.Key, item.Value);
                        }
                    }
                    //添加返回参数
                    if (outPutParms != null)
                    {
                        foreach (DictionaryEntry item in outPutParms)
                        {
                            //需要后期判断参数类型 得到 DbType
                            parems.Add("@" + item.Key, item.Value, DbType.Int32, ParameterDirection.Output);
                            //这样写返回值可能会出错
                            //parems.Add("@" + item.Key, ParameterDirection.Output);
                        }
                    }
                    List<T> lists = Connection.Query<T>(storedProcedureName, parems, null, true, null, CommandType.StoredProcedure).ToList();
                    string[] keys = new string[outPutParms.Keys.Count];
                    outPutParms.Keys.CopyTo(keys, 0);
                    foreach (string item in keys)
                    {
                        outPutParms[item] = parems.Get<int>("@" + item);
                    }
                    return lists;
                }
            }
            catch (Exception)
            {
                return null;
            }
        }

        /// <summary>
        /// 根据输入参数和输出参数的存储过程
        /// </summary>
        /// <typeparam name="T">实体类型</typeparam>
        /// <param name="storedProcedureName">存储过程名称</param>
        /// <param name="normalParas">普通参数 键值对</param>
        /// <returns>查询结果泛型集合</returns>
        public List<T> SelectByStoredProcedure<T>(string storedProcedureName, Hashtable normalParms)
        {
            try
            {
                using (DbConnection Connection = new DBFactory().GetInstance())
                {
                    var parems = new DynamicParameters();
                    //添加普通参数
                    if (normalParms != null)
                    {
                        foreach (DictionaryEntry item in normalParms)
                        {
                            parems.Add("@" + item.Key, item.Value);
                        }
                    }
                    List<T> lists = Connection.Query<T>(storedProcedureName, parems, null, true, null, CommandType.StoredProcedure).ToList();
                    return lists;
                }
            }
            catch (Exception ex)
            {
                return null;
            }
        }

        /// <summary>
        /// 分页查询
        /// </summary>
        /// <param name="pageIndex">第几页</param>
        /// <param name="pageSize">每页大小</param>
        /// <param name="where">where条件 首条件不含and</param>
        /// <param name="orderBy">排序方式(可以多排序逗号分割) 如：CreateDate DESC</param>
        /// <param name="pageCount">输出结果集总数</param>
        /// <returns>查询集合</returns>
        public List<T> SelectListByPage<T>(int pageIndex, int pageSize, string where, string orderBy, ref int pageCount, string columns = "")
        {
            try
            {
                var parems = new DynamicParameters();
                Type typeFromHandle = typeof(T);
                string strTableName = typeFromHandle.Name;
                ///update ywj
                Hashtable normalParms = new Hashtable();
                if (columns == "")
                {
                    normalParms.Add("Column", "*");
                }
                else
                {
                    normalParms.Add("Column", columns);
                }
                normalParms.Add("tableName", strTableName);
                normalParms.Add("orderBy", orderBy);
                normalParms.Add("where", where);
                normalParms.Add("pageIndex", pageIndex);
                normalParms.Add("pageSize", pageSize);

                Hashtable outputParms = new Hashtable();
                outputParms.Add("pageCount", 0);

                List<T> list = SelectByStoredProcedure<T>("P_SelectListByPage", normalParms, ref outputParms).ToList();
                pageCount = Convert.ToInt32(outputParms["pageCount"]);
                return list;
            }
            catch (Exception ex)
            {
                return null;
            }
        }

        /// <summary>
        /// 根据输入参数和输出参数的存储过程
        /// </summary>
        /// <typeparam name="T">实体类型</typeparam>
        /// <param name="storedProcedureName">存储过程名称</param>
        /// <param name="normalParas">普通参数 键值对</param>
        /// <param name="outPutParas">输出参数 键值对</param>
        /// <returns>查询结果泛型集合</returns>
        public int SelectByStored(string storedProcedureName, Hashtable normalParms, ref Hashtable outPutParms)
        {
            using (DbConnection Connection = new DBFactory().GetInstance())
            {
                var parems = new DynamicParameters();
                //添加普通参数
                if (normalParms != null)
                {
                    foreach (DictionaryEntry item in normalParms)
                    {
                        parems.Add("@" + item.Key, item.Value);
                    }
                }
                //添加返回参数
                if (outPutParms != null)
                {
                    foreach (DictionaryEntry item in outPutParms)
                    {
                        //需要后期判断参数类型 得到 DbType
                        parems.Add("@" + item.Key, item.Value, DbType.Int32, ParameterDirection.Output);
                        //这样写返回值可能会出错
                        //parems.Add("@" + item.Key, ParameterDirection.Output);
                    }
                }
                var retmes = Connection.QueryMultiple(storedProcedureName, parems, null, null, CommandType.StoredProcedure);

                string[] keys = new string[outPutParms.Keys.Count];
                outPutParms.Keys.CopyTo(keys, 0);
                foreach (string item in keys)
                {
                    outPutParms[item] = parems.Get<int>("@" + item);
                }

                var m = retmes.Read<int>().Single();
                return m;
            }
        }

        /// <summary>
        /// 根据输入参数和输出参数的存储过程
        /// </summary>
        /// <typeparam name="T">实体类型</typeparam>
        /// <param name="storedProcedureName">存储过程名称</param>
        /// <param name="normalParas">普通参数 键值对</param>
        /// <param name="outPutParas">输出参数 键值对</param>
        /// <returns>查询结果泛型集合</returns>
        public void SelectByStoredNoReturn(string storedProcedureName, Hashtable normalParms, ref Hashtable outPutParms)
        {
            using (DbConnection Connection = new DBFactory().GetInstance())
            {
                var parems = new DynamicParameters();
                //添加普通参数
                if (normalParms != null)
                {
                    foreach (DictionaryEntry item in normalParms)
                    {
                        parems.Add("@" + item.Key, item.Value);
                    }
                }
                //添加返回参数
                if (outPutParms != null)
                {
                    foreach (DictionaryEntry item in outPutParms)
                    {
                        //需要后期判断参数类型 得到 DbType
                        parems.Add("@" + item.Key, item.Value, DbType.Int32, ParameterDirection.Output);
                        //这样写返回值可能会出错
                        //parems.Add("@" + item.Key, ParameterDirection.Output);
                    }
                }
                var retmes = Connection.QueryMultiple(storedProcedureName, parems, null, null, CommandType.StoredProcedure);

                string[] keys = new string[outPutParms.Keys.Count];
                outPutParms.Keys.CopyTo(keys, 0);
                foreach (string item in keys)
                {
                    outPutParms[item] = parems.Get<int>("@" + item);
                }
            }
        }

        #endregion

        /// <summary>
        /// 必须实现的接口成员
        /// </summary>
        public virtual void Dispose() { }
    }
}