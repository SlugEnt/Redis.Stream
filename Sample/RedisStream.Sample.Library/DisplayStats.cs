using Spectre.Console;


namespace SlugEnt.SLRStreamProcessing.Sample;

public class DisplayStats
{
    protected Layout _layout;
    protected Table  _statsTable;
    protected Table  _menuTable;
    protected Table  _lastUpdatedTable;

    private int _columnCount;

    private Dictionary<string, string> _menuItems = new();


    public DisplayStats()
    {
        //Layout x = new Layout("hi").;

        _layout = new Layout("Main")
            .SplitRows(new Layout("Top")
                           .SplitColumns(
                                         new Layout("Menu").Size(48),
                                         new Layout("Stats").Size(84)),
                       new Layout("Bottom"));
        ;

        _statsTable             = new Table().Centered();
        _statsTable.ShowHeaders = true;
        _columnCount            = 0;

        _menuTable = new Table();
        _menuTable.AddColumn("Menu Item");

        _lastUpdatedTable             = new Table();
        _lastUpdatedTable.ShowHeaders = false;
        _lastUpdatedTable.AddColumn("last");
        _lastUpdatedTable.AddRow("Never");

        // Add Table to Stats Panel
        _layout["Stats"].Update(_statsTable);
        _layout["menu"].Update(_menuTable);
        _layout["bottom"].Update(_lastUpdatedTable);
    }


    public void AddMenuItem(string key, string name) { _menuItems.Add(key, name); }


    public void DisplayMenu()
    {
        foreach (KeyValuePair<string, string> menuItem in _menuItems)
        {
            _menuTable.AddRow(MarkUpValue($" ( {menuItem.Key} )  {menuItem.Value}", "yellow"));
        }


        _layout["menu"].Update(_menuTable);
    }


    public void AddRow(string rowTitle) { _statsTable.AddRow(MarkUpValue(rowTitle, "green")); }


    public void AddColumn(string columnTitle, int padding = 0)
    {
        _statsTable.AddColumn(columnTitle);
        if (padding > 0)
            _statsTable.Columns[_columnCount].PadRight(padding);
    }


    /// <summary>
    /// Must Override - Is used to update the Stats Display
    /// </summary>
    protected virtual void UpdateData() { }



    protected string MarkUpValue(string value, string colorName, bool bold = false, bool underline = false,
                                 bool italic = false)
    {
        string val = "[" + colorName + "]";
        if (bold)
            val += "[bold]";


        val += value + "[/]";
        return val;
    }


    protected string MarkUp(ulong value, bool positiveGreen = true)
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


    protected string MarkUp(int value, bool positiveGreen = true)
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


    protected string MarkUp(bool value)
    {
        string color = "";
        if (value)
            color = "green";
        else
            color = "red";

        string val = "[" + color + "]";
        val += value + "[/]";
        return val;
    }


    public void Refresh()
    {
        UpdateData();

        // Update Last Updated Row.
        _lastUpdatedTable.UpdateCell(0, 0, "Last Updated:  " + DateTime.Now.ToString());

        AnsiConsole.Clear();
        AnsiConsole.Write(_layout);
    }
}