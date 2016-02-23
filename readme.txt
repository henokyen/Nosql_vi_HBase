This python program implements a simple twitter like application. It allows
users to register, tweet, follow other users and log in to view tweets. The
backend database is NoSql in HBase, thus, all information about a single user,
i.e., the user profile, tweets, people who follows the user, people who the
user follows, are all is in one big table, making the information retrieval
process very fast.
