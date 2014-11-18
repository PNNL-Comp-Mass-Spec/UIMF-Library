using System;
using System.Collections.Generic;
using System.Globalization;

namespace UIMFLibrary
{
    /// <summary>
    /// Container for a set of frame parameters
    /// </summary>
    public class FrameParams
    {
        #region Member Variables
        private readonly Dictionary<FrameParamKeyType, FrameParam> mFrameParameters;
#endregion

        #region Properties
        public Dictionary<FrameParamKeyType, FrameParam> Values 
        {
            get { return mFrameParameters; }
        }

        public double CalibrationSlope {
            get {
                if (!HasParameter(FrameParamKeyType.CalibrationSlope))
                    return 0;

                return GetValueDouble(FrameParamKeyType.CalibrationSlope);
            }
        }

        public double CalibrationIntercept
        {
            get
            {
                if (!HasParameter(FrameParamKeyType.CalibrationIntercept))
                    return 0;

                return GetValueDouble(FrameParamKeyType.CalibrationIntercept);
            }
        }
        public DataReader.FrameType FrameType
        {
            get
            {
                if (!HasParameter(FrameParamKeyType.FrameType))
                    return DataReader.FrameType.MS1;

                int frameType = GetValueInt32(FrameParamKeyType.FrameType);
                if (frameType == 0)
                {
                    // This is an older UIMF file where the MS1 frames were labeled as 0
                    return DataReader.FrameType.MS1;
                }

                return (DataReader.FrameType)frameType;
            }
        }

        public int Scans
        {
            get
            {
                if (!HasParameter(FrameParamKeyType.Scans))
                    return 0;

                return GetValueInt32(FrameParamKeyType.Scans);
            }
        }

        #endregion

        /// <summary>
        /// Constructor
        /// </summary>
        public FrameParams()
        {
            mFrameParameters = new Dictionary<FrameParamKeyType, FrameParam>();
        }

        public void AddUpdateValue(FrameParamKeyType paramType, double value)
        {
            AddUpdateValue(paramType, value.ToString(CultureInfo.InvariantCulture));
        }

        public void AddUpdateValue(FrameParamKeyType paramType, int value)
        {
            AddUpdateValue(paramType, value.ToString(CultureInfo.InvariantCulture));
        }

        public void AddUpdateValue(FrameParamKeyType paramType, string value)
        {
            FrameParam paramEntry;
            if (mFrameParameters.TryGetValue(paramType, out paramEntry))
            {
                paramEntry.Value = value;
            }
            else
            {
                paramEntry = new FrameParam(FrameParamUtilities.GetParamDefByType(paramType), value);
                mFrameParameters.Add(paramType, paramEntry);
            }
        }

        public void AddUpdateValue(FrameParamDef paramDef, double value)
        {
            AddUpdateValue(paramDef, value.ToString(CultureInfo.InvariantCulture));
        }

        public void AddUpdateValue(FrameParamDef paramDef, int value)
        {
            AddUpdateValue(paramDef, value.ToString(CultureInfo.InvariantCulture));
        }

        public void AddUpdateValue(FrameParamDef paramDef, string value)
        {
            FrameParam paramEntry;
            if (mFrameParameters.TryGetValue(paramDef.ParamType, out paramEntry))
            {
                paramEntry.Value = value;
            }
            else
            {
                paramEntry = new FrameParam(paramDef, value);
                mFrameParameters.Add(paramDef.ParamType, paramEntry);
            }
        }
        
        public string GetValue(FrameParamKeyType paramType)
        {
            return GetValue(paramType, string.Empty);
        }

        public string GetValue(FrameParamKeyType paramType, string valueIfMissing)
        {
            FrameParam paramEntry;
            if (mFrameParameters.TryGetValue(paramType, out paramEntry))
            {
                return paramEntry.Value;
            }

            return valueIfMissing;
        }

        public double GetValueDouble(FrameParamKeyType paramType)
        {
            return GetValueDouble(paramType, 0.0);
        }

        public double GetValueDouble(FrameParamKeyType paramType, double valueIfMissing)
        {
            FrameParam paramEntry;
            if (mFrameParameters.TryGetValue(paramType, out paramEntry))
            {
                double result;
                if (Double.TryParse(paramEntry.Value, out result))
                    return result;
            }

            return valueIfMissing;
        }

        public int GetValueInt32(FrameParamKeyType paramType)
        {
            return GetValueInt32(paramType, 0);
        }

        public int GetValueInt32(FrameParamKeyType paramType, int valueIfMissing)
        {
            FrameParam paramEntry;
            if (mFrameParameters.TryGetValue(paramType, out paramEntry))
            {
                int result;
                if (Int32.TryParse(paramEntry.Value, out result))
                    return result;
            }

            return valueIfMissing;
        }

        public bool HasParameter(FrameParamKeyType paramType)
        {
            return mFrameParameters.ContainsKey(paramType);
        }
    }
}
