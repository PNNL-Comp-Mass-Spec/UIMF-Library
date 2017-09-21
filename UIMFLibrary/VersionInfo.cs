using System;

namespace UIMFLibrary
{
    /// <summary>
    /// Information about the UIMF file - format and creating software
    /// </summary>
    public class VersionInfo
    {
        /// <summary>
        /// Version row id
        /// </summary>
        public int VersionId { get; set; }

        /// <summary>
        /// UIMF file schema version
        /// </summary>
        public Version UimfVersion { get; set; }

        /// <summary>
        /// Creating software name
        /// </summary>
        public string SoftwareName { get; set; }

        /// <summary>
        /// Creating software version
        /// </summary>
        public Version SoftwareVersion { get; set; }

        /// <summary>
        /// Date the row was entered
        /// </summary>
        public DateTime DateEntered { get; set; }

        /// <summary>
        /// Report the UimfVersion
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            return UimfVersion.ToString();
        }
    }
}
