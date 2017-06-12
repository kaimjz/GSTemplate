using System;

namespace Models
{
    /// <summary>
    /// 标识主键
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    public class PrimaryKeyAttribute : Attribute
    {
        public PrimaryKeyAttribute()
        {
        }
    }

    /// <summary>
    /// 扩展字段
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    public class ExtensionAttribute : Attribute
    {
        public ExtensionAttribute()
        {
        }
    }
}