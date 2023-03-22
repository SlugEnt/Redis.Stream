using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using RabbitMQ.Stream.Client;
using Redis.Stream.Sample;
using SlugEnt.MQStreamProcessor;

namespace Redis.Stream.Sample;

public static class HelperFunctions
{
    /// <summary>
    /// Used to create incrementing Flight days with letter naming.  So, A, Z, AAC, FRZ
    /// </summary>
    /// <param name="currentDayCode"></param>
    /// <returns></returns>
    public static string NextFlightDay(string currentDayCode)
    {
        byte   z            = (byte)'Z';
        byte[] flightDayIDs = Encoding.ASCII.GetBytes(currentDayCode);

        int  lastIndex       = flightDayIDs.Length - 1;
        int  currentIndex    = lastIndex;
        bool continueLooping = true;

        while (continueLooping)
        {
            if (flightDayIDs[currentIndex] == z)
            {
                if (currentIndex == 0)
                {
                    // Append a new column
                    flightDayIDs[currentIndex] = (byte)'A';
                    string newflightDay = Encoding.ASCII.GetString(flightDayIDs) + "A";
                    return newflightDay;
                }

                // Change this index to A and move to the prior index.
                flightDayIDs[currentIndex] = (byte)'A';
                currentIndex--;
            }

            // Just increment this index to next letter
            else
            {
                flightDayIDs[currentIndex]++;
                return Encoding.ASCII.GetString(flightDayIDs);
            }
        }

        // Should never get here.
        return currentDayCode;
    }


    /// <summary>
    /// Retrieves the Flight Day from the Confirmation Message
    /// </summary>
    /// <param name="e"></param>
    /// <returns></returns>
    public static string GetFlightDay(MessageConfirmationEventArgs e)
    {
        string flightDay = "";
        if (e.Message.ApplicationProperties.ContainsKey(SampleCommon.AP_DAY))
        {
            flightDay = (string)e.Message.ApplicationProperties[SampleCommon.AP_DAY];
        }
        else
            flightDay = "Not Specified";

        return flightDay;
    }
}