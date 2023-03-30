using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SlugEnt.SLRStreamProcessing.Sample;

public static class SpectreAddons
{
    public static string MarkUpValue(string value, string colorName, bool bold = false, bool underline = false,
                                     bool italic = false)
    {
        string val = "[" + colorName + "]";
        if (bold)
            val += "[bold]";


        val += value + "[/]";
        return val;
    }


    public static string MarkUp(ulong value, bool positiveGreen = true)
    {
        string color = "green";
        if (!positiveGreen)
        {
            if (value > 0)
                color = "red";
        }

        string val = "[" + color + "]";
        val += value + "[/]";
        return val;
    }


    public static string MarkUp(int value, bool positiveGreen = true)
    {
        string color = "green";
        if (!positiveGreen)
        {
            if (value > 0)
                color = "red";
        }

        string val = "[" + color + "]";
        val += value + "[/]";
        return val;
    }


    public static string MarkUp(bool value, bool trueGreen = true)
    {
        string color = "";
        if (trueGreen)
            color = "green";
        else
            color = "red";

        string val = "[" + color + "]";
        val += value + "[/]";
        return val;
    }
}