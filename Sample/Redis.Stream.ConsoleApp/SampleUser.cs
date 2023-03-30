namespace SlugEnt.SLRStreamProcessing.Sample;

public class SampleUser
{
    public SampleUser(string name, int age, bool isFemale)
    {
        Name     = name;
        Age      = age;
        IsFemale = isFemale;
    }


    public string Name { get; set; }
    public int Age { get; set; }
    public bool IsFemale { get; set; }
}