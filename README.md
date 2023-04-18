# ShopRecommend

This project is a project that I completed during my training in Kunshan Jiepu Co., Ltd. The main structure of the project is in the package \src\main\java\com\briup\Pro_recommend, and other written items are test items written by me during the project learning process.

There are nine steps in the main project: 1. Obtain the user's preference value for the product; 2. Organize the preference data into a preference value matrix; 3. Count the co-occurrence of the product according to the results of the first step, that is, all Do the Cartesian product of the product; 4. Count the number of co-occurrences of the product, that is, the number of occurrences of the two co-occurring products may appear multiple times; 5. Obtain the product co-occurrence matrix according to the results of the fourth step; 6. For the first The data obtained in the second step and the fifth step are matrix multiplied to obtain the recommended value; 7. The results obtained in the sixth step are counted, that is, the user and the product are the same, and the recommended value is accumulated; 8. The recommended value in the seventh step is calculated Certain cleaning is performed to remove purchased products and leave the final recommended data; 9. Save the generated recommended data to the database.

The system is built with Ubuntu20.04, Hadoop3.0.3 cluster (Master master node, Slave slave node), distributed file management system, jdk1.8, MySQL5.7.35.
