from mrjob.job import MRJob
from mrjob.job import MRStep
from math import sqrt
from itertools import combinations

class MovieSimilarities(MRJob):
    def configure_args(self):
        super(MovieSimilarities,self).configure_args()
        self.add_file_arg('--items')

    def load_movie_names(self):
        self.movieNames={}
        with open('u.item',encoding='ascii',errors='ignore') as f:
            for line in f:
                fields=line.split('|')
                self.movieNames[int(fields[0])]=fields[1]

    def steps(self):
        return [
            MRStep(mapper=self.mapper,reducer=self.reducer),
            MRStep(mapper=self.mapper_create_item_pairs,reducer=self.reducer_compute_similarity),
            MRStep(mapper=self.mapper_sort_similarities,mapper_init=self.load_movie_names,
                   reducer=self.reducer_output_similarties)

        ]

    def mapper(self,key,line):
        (user_id,movie_id,rating,timestamp)=line.split('\t')
        yield user_id,(movie_id,float(rating))

    def reducer(self,user_id,itemRatings):
        ratings=[]
        for (movieId,rating) in itemRatings:
            ratings.append((movieId,rating))
        yield user_id,ratings

    def mapper_create_item_pairs(self,user_id,itemRatings):
        # "combinations" finds every possible pair from the list of movies
        # this user viewed.
        for itemRatings1,itemRatings2 in combinations(itemRatings,2):
            movieID1=itemRatings1[0]
            rating1=itemRatings1[1]
            movieID2=itemRatings2[0]
            rating2=itemRatings2[1]

            yield (movieID1,movieID2),(rating1,rating2)
            yield (movieID2,movieID1),(rating2,rating1)

    def cosine_similarity(self,ratingPairs):
        # Computes the cosine similarity metric between two
        # rating vectors.
        numPairs=0
        sum_xx=sum_yy=sum_xy=0
        for ratingX,ratingY in ratingPairs:
            sum_xx+=ratingX*ratingX
            sum_yy+=ratingY*ratingY
            sum_xy+=ratingX*ratingY
            numPairs+=1
        numerator=sum_xy
        denominator=sqrt(sum_xx)*sqrt(sum_yy)

        score=0
        if (denominator):
            score=(numerator/(float(denominator)))
        return (score,numPairs)

    def reducer_compute_similarity(self,moviePair,ratingPairs):

        #calculating similarity between ratings pairs or vectors
        score,numPairs=self.cosine_similarity(ratingPairs)

        # Enforce a minimum score and minimum number of co-ratings
        # to ensure quality
        if (numPairs>10 and score>0.95):
            yield moviePair,(score,numPairs)

    def mapper_sort_similarities(self,moviePair,scores):
        score,n=scores
        movie1,movie2=moviePair

        yield (self.movieNames[int(movie1)],score), \
              (self.movieNames[int(movie2)],n)

    def reducer_output_similarties(self,movieScore,similarN):
        movie1,score=movieScore
        for movie2,n in similarN:
            yield movie1,(movie2,score,n)


if __name__=="__main__":
    MovieSimilarities.run()






