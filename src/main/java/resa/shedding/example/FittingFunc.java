package resa.shedding.example;

import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by kailin on 2017/7/20.
 */
public class FittingFunc {
    public static Logger LOG = LoggerFactory.getLogger(FittingFunc.class);

    /**
     * 回带计算X值
     *
     * @param guass
     * @param count
     * @return
     */
    private static double[] get_Value(double[][] guass, int count) {
        double[] x = new double[count];
        double[][] X_Array = new double[count][count];

        for (int i = 0; i < count; i++)
            for (int j = 0; j < count; j++)
                X_Array[i][j] = guass[i][j];

        if (2 < count - 1)// 表示有多解
        {
            return null;
        }
        // 回带计算x值
        x[count - 1] = guass[count - 1][count] / guass[count - 1][count - 1];
        for (int i = count - 2; i >= 0; i--) {
            double temp = 0;
            for (int j = i; j < count; j++) {
                temp += x[j] * guass[i][j];
            }
            x[i] = (guass[i][count] - temp) / guass[i][i];
        }

        return x;
    }

    /**
     * 最小二乘法部分， 计算增广矩阵
     *
     * @param guass
     * @param count
     * @return
     */
    private static double[] cal_Guass(double[][] guass, int count) {
        double temp;
        double[] x_value;

        for (int j = 0; j < count; j++) {
            int k = j;
            double min = guass[j][j];

            for (int i = j; i < count; i++) {
                if (Math.abs(guass[i][j]) < min) {
                    min = guass[i][j];
                    k = i;
                }
            }

            if (k != j) {
                for (int x = j; x <= count; x++) {
                    temp = guass[k][x];
                    guass[k][x] = guass[j][x];
                    guass[j][x] = temp;
                }
            }

            for (int m = j + 1; m < count; m++) {
                double div = guass[m][j] / guass[j][j];
                for (int n = j; n <= count; n++) {
                    guass[m][n] = guass[m][n] - guass[j][n] * div;
                }
            }
        }
        x_value = get_Value(guass, count);

        return x_value;
    }

    /**
     * 计算∑(x^j)*y
     *
     * @param y
     * @param x
     * @param order
     * @return
     */
    private static double cal_multi(double[] y, double[] x, int order) {
        double result = 0;

        int length = x.length;

        for (int i = 0; i < length; i++) {
            result += Math.pow(x[i], order) * y[i];
        }

        return result;
    }

    /**
     * 累加的计算
     *
     * @param input
     * @param order
     * @return
     */
    private static double cal_sum(double[] input, int order) {
        double result = 0;
        int length = input.length;

        for (int i=0; i<length; i++) {
            result += Math.pow(input[i], order);
        }

        return result;
    }

    /**
     * 得到数据的法矩阵,输出为发矩阵的增广矩阵
     *
     * @param y
     * @param x
     * @param n
     * @return
     */
    private static double[][] get_Array(double[] y, double[] x, int n) {
        double[][] result = new double[n + 1][n + 2];

        if (y.length != x.length) {
            System.out.println("两个输入数组长度不一！");
        }

        for (int i = 0; i <= n; i++) {
            for (int j = 0; j <= n; j++) {
                result[i][j] = cal_sum(x, i + j);
            }
            result[i][n + 1] = cal_multi(y, x, i);
        }

        return result;
    }


    /**
     * 多项式拟合函数,输出系数是y=a0+a1*x+a2*x*x+.........，按a0,a1,a2输出
     *
     * @param y
     * @param x
     * @param order
     * @return
     */
    public static double[] polyfit(double[] y, double[] x, int order) {
        double[][] guass = get_Array(y, x, order);

        double[] ratio = cal_Guass(guass, order + 1);

        return ratio;
    }


    /**
     * 一次拟合函数，y=a0+a1*x,输出次序是a0,a1
     *
     * @param y
     * @param x
     * @return
     */
    public static double[] linear(double[] y, double[] x) {
        double[] ratio = polyfit(y, x, 1);
        return ratio;
    }

    /**
     * 对数拟合函数,.y= c*(ln x)+b,输出为b,c
     *
     * @param y
     * @param x
     * @return
     */
    public static double[] logest(double[] y, double[] x) {
        double[] lnX = new double[x.length];

        for (int i = 0; i < x.length; i++) {
            if (x[i] == 0 || x[i] < 0) {
                LOG.error("Negative numbers can not be calculated logarithm");
                return null;
            }
            lnX[i] = Math.log(x[i]); /// Math.log(1.001378542);
        }

        return linear(y, lnX);
    }

    public static double[] hyperbola(List<Pair<Double, Double>> list) {
        double[] X = new double[list.size()];
        double[] Y = new double[list.size()];
        for (int i = 0; i < list.size(); i++) {
            X[i] = 1.0 / -(list.get(i).getValue0());
            Y[i] = list.get(i).getValue1();
        }
        return linear(Y,X);
    }

    public static int count = 0;
    public static void main(String[] args) {
        double[] x = {-1168.0004973654445,-128.74632375186934,-35.76267017877404,-26.251339218419425,
                -11.233803891875164,-580.1916233904778,-196.9378957982446,-178.4704654987978,-161.37560501126762,
                -94.22740601385776,-69.74884405853582,-55.80148626745969,-55.791602028953214,-54.133536036151035,
                -53.58673460952915, -54.44622937183436,-56.157701993800835};
        double[] y = {
                642.3242074927954,
                1518.140848141284,
                2972.2781039356382,
                5190.734896089098,
                7270.217952304768,
                10886.752786885247,
                11447.061614619986,
                11378.211886009562,
                11480.47142219894,
                12411.82916106926,
                12985.998927003406,
                13271.440397658242,
                13262.901207295414,
                13376.37273391354,
                13432.38508171412,
                13293.252571609431,
                13276.23411662315
        };
        List<Pair<Double,Double>> aa = new ArrayList<>();

        for (int j=0; j<10; j++) {
            aa.add(new Pair<Double,Double>(x[j],y[j]));
        }
        double[] result = FittingFunc.hyperbola(aa);
        for (double d : result) {
            System.out.println(d+"~");
            //System.out.println(1000 / (-Math.log(Math.random()) * 1000.0 / 50));
            //System.out.println(1000 / (-Math.log(Math.random()) * 1000.0 / 200));
        }
        System.out.println(result[0]+(result[1] * 1.0/-(-1)));

        //System.out.println(Math.log(840.0 * 1000.0) / Math.log(1.001378542));

    }

}
