
namespace UIMFLibrary
{
    /// <summary>
    /// Frame parameter container
    /// </summary>
    public class FrameParam
    {
        /// <summary>
        /// Frame parameter definition
        /// </summary>
        public FrameParamDef Definition { get; set; }

        /// <summary>
        /// Parameter value
        /// </summary>
        public dynamic Value { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="paramDef">Frame parameter definition</param>
        /// <param name="value">Parameter value</param>
        public FrameParam(FrameParamDef paramDef, dynamic value)
        {
            Definition = paramDef;
            Value = value;
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="paramDef">Frame parameter definition</param>
        /// <param name="value">Parameter value</param>
        public FrameParam(FrameParamDef paramDef, string value)
        {
            Definition = paramDef;
            Value = FrameParamUtilities.ConvertStringToDynamic(paramDef.DataType, value);
        }

        /// <summary>
        /// Customized ToString()
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            return Value == null ? Definition.ToString() : Value.ToString();
        }
    }
}
