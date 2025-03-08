const pipeline = [
    {
      $lookup: {
        from: "trakt_ratings",
        let: { movie_id: "$tmdb_id" },
        pipeline: [
          {
            $match: {
              $expr: {
                $eq: ["$tmdb_id", "$$movie_id"]
              }
            }
          },
          {
            $project: {
              _id: 0,
              rated_at: 1,
              rating: 1,
              user: 1
            }
          }
        ],
        as: "ratings"
      }
    },
    {
      $project: {
        _id: 1,
        title: 1,
        release_date: 1,
        popularity: 1,
        vote_average: 1,
        vote_count: 1,
        overview: 1,
        tmdb_id: 1,
        ratings: 1
      }
    },
    {
      $merge: {
        into: "movies_with_ratings",
        whenMatched: "replace",
        whenNotMatched: "insert"
      }
    }
  ];

  db.tmbd_movie_details.aggregate(pipeline);