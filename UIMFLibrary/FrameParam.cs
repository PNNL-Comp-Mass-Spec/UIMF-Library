using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace UIMFLibrary
{
    public class FrameParam
    {
        public FrameParamDef Definition { get; set; }
        public string Value { get; set; }

        public FrameParam(FrameParamDef paramDef, string value)
        {
            Definition = paramDef;
            Value = value;
        }
    }
}
