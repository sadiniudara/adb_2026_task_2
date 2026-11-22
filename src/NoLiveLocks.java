import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 *
 *
 */
public class NoLiveLocks {


    /**
     */

    }

        }
    }


            con.setAutoCommit(false);

                for (int i = 0; i < num_tuple; i++) {
                    ps.setInt(1, i);
                    ps.setInt(3, 0);
                    ps.addBatch();
            }

            con.commit();
        } catch (SQLException e) {
        }
    }



            }

    }

            try {
                        }
                }
                        }

        }

        /**
         */

                        }
                    }

    /**
     */
        @Override
        public void run() {
                    try {

            while (true) {
                try {
                    con.commit();
                } catch (SQLException e) {
                    attempt++;
                } finally {
                    }
                backoff(attempt);
            }
        }
    }

            }
        }
    }

    static void backoff(int attempt) {
    }

    static void updateMax(AtomicInteger max, int value) {
        int cur;
    }

    }
