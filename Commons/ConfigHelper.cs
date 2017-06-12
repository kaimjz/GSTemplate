using System.Configuration;

namespace Commons
{
    /// <summary>
    /// config帮助类
    /// </summary>
    public class ConfigHelper
    {
        /// <summary>
        /// 根据Key取Value值
        /// </summary>
        /// <param name="key"></param>
        public static string GetValue(string key)
        {
            if (key == "isEncrypt" && ConfigurationManager.AppSettings["isEncrypt"] == null)
            {
                return "False";
            }
            else
            {
                string mes = (ConfigurationManager.AppSettings[key] + "").Trim();
                return mes;
            }
        }
    }
}