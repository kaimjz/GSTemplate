using Commons;
using DAL;
using Models;

namespace BLL
{
    public class Users_BLL
    {
        /// <summary>
        /// demo
        /// </summary>
        /// <param name="userM"></param>
        /// <returns></returns>
        public Enums.TickLingEnum InsertEntity(Users userM)
        {
            using (var dal = new Users_DAL())
            {
                return dal.Insert(userM);
            }
        }
    }
}