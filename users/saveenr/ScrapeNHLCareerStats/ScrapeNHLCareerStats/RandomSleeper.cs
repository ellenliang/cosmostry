using System;

namespace ScrapeNHLCareerStats
{
    public class RandomSleeper
    {
        Random rans;
        private int MaxMiliseconds;
        public RandomSleeper( int max_miliseconds)
        {
            this.rans = new System.Random();
            this.MaxMiliseconds = max_miliseconds;
        }

        public void Sleep()
        {
            double wait = rans.NextDouble() * MaxMiliseconds;
            System.Threading.Thread.Sleep((int)wait);
        }
    }
}