// --------------------------------------------------------------------------------------------------------------------
// <copyright file="LogEntry.cs" company="">
//   
// </copyright>
// <summary>
//   Defines the LogEntry type.
// </summary>
// --------------------------------------------------------------------------------------------------------------------



namespace UIMFLibrary
{
	using System;

	/// <summary>
	/// TODO The log entry.
	/// </summary>
	public class LogEntry
	{
		public string PostedBy { get; set; }
		public string Type { get; set; }
		public string Message { get; set; }
		public DateTime PostingTime { get; set; }
	}
}
