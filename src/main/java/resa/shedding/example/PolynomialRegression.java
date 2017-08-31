package resa.shedding.example;

import org.apache.commons.math3.fitting.PolynomialCurveFitter;
import org.apache.commons.math3.fitting.WeightedObservedPoints;
import org.javatuples.Pair;
import resa.shedding.basicServices.api.LearningModel;

import java.util.List;

/**
 * Created by kailin on 27/3/17.
 */
public class PolynomialRegression extends LearningModel {

    public static void main(String[] args) {
        PolynomialRegression polynomialRegression = new PolynomialRegression();
        polynomialRegression.FitTest();
    }

    private void FitTest() {
        final WeightedObservedPoints obs = new WeightedObservedPoints();
//        obs.add(Math.log10(644),Math.log10(67));
//        obs.add(Math.log10(833),Math.log10(65));
//        obs.add(Math.log10(655),Math.log10(65));
//        obs.add(Math.log10(489),Math.log10(62));
//        obs.add(Math.log10(800),Math.log10(69));
        obs.add( 350.45057293169015,560.6339468302658);
        obs.add(314.48542189206825,299.51461988304095);
        obs.add(367.9680190640623,357.74468085106383);
        obs.add(434.2420344939902,385.7920997920998);
        obs.add(497.2413696866634,423.3119266055046);
        obs.add(497.69408806647317,438.5176252319109);
        obs.add(520.0761244496716,482.09689213893967);
        obs.add(554.6070269779332,495.75551782682516);

        obs.add(614.300116354013,517.741935483871);
        obs.add(758.3246558140481,647.2422451994091);
        obs.add(894.4771630116282,728.8885672937771);
        obs.add(935.2068909327313,749.8005578800557);
        obs.add(1066.5215408756264,762.0137362637363);
        obs.add(2520.752398812955,1389.498730964467);
        obs.add(53144.501255564595,2922.772874058127);

        obs.add(0,0);obs.add(0,0);obs.add(0,0);obs.add(0,0);obs.add(0,0);obs.add(0,0);
        obs.add(0,0);obs.add(0,0);obs.add(0,0);obs.add(0,0);obs.add(0,0);obs.add(0,0);

        // Instantiate a third-degree polynomial fitter.
        final PolynomialCurveFitter fitter = PolynomialCurveFitter.create(1);

        // Retrieve fitted parameters (coefficients of the polynomial function).
        final double[] coeff = fitter.fit(obs.toList());
        for(int i=0;i<coeff.length;i++)
            System.out.println(coeff[i]);
    }

    @Override
    public double[] Fit(Object[] objects) {
        List list = (List) objects[0];
        int order = (int) objects[1];
        boolean three = (boolean) objects[2];
        final WeightedObservedPoints obs = new WeightedObservedPoints();
        //System.out.println("list.size = "+list.size());
        for(int i=0; i<list.size();i++) {
            if (three) {
                System.out.println("pair0 in: " + ((Pair<Double, Double>) list.get(i)).getValue0());
                System.out.println("pair1 out: " + ((Pair<Double, Double>) list.get(i)).getValue1());
            }
            if (!three) {
                obs.add(((Pair<Double, Double>) list.get(i)).getValue0() / 100, ((Pair<Double, Double>) list.get(i)).getValue1() / 100);
            } else {
                obs.add(((Pair<Double, Double>) list.get(i)).getValue0(), ((Pair<Double, Double>) list.get(i)).getValue1());
            }
        }
        if (three) {
            System.out.println("buildddddddddddddddddddddddddddddd~~~~~~~~~~~~~~~~~~~~~~~");
        }
        obs.add(0,0);obs.add(0,0);obs.add(0,0);obs.add(0,0);obs.add(0,0);obs.add(0,0);
        obs.add(0,0);obs.add(0,0);obs.add(0,0);obs.add(0,0);obs.add(0,0);obs.add(0,0);
        obs.add(0,0);obs.add(0,0);obs.add(0,0);obs.add(0,0);obs.add(0,0);obs.add(0,0);
        // Instantiate a third-degree polynomial fitter.
        final PolynomialCurveFitter fitter = PolynomialCurveFitter.create(order);

        // Retrieve fitted parameters (coefficients of the polynomial function).
        final double[] coeff = fitter.fit(obs.toList());
        return coeff;
    }
}
