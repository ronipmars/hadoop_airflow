import com.opencsv.CSVReader;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.io.StringReader;

public class FraudAnalysisMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text pattern = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Usando CSVReader para dividir corretamente as colunas mesmo que tenham vírgulas dentro de campos
        CSVReader reader = new CSVReader(new StringReader(value.toString()));
        String[] tokens;

        try {
            tokens = reader.readNext();  // Lê a próxima linha do CSV
            if (tokens == null) {
                return;  // Ignora linhas vazias
            }

            // Extraindo os campos relevantes com base nos índices corretos
            String timestamp = tokens[1];  // Ex: 2020-06-21 12:13:36
            String amountStr = tokens[5];  // Ex: 105.93
            String category = tokens[4];   // Ex: food_dining
            String location = tokens[10];  // Ex: High Rolls Mountain Park, NM

            // Validando e convertendo o valor da transação para float
            float transactionAmount;
            try {
                transactionAmount = Float.parseFloat(amountStr);
            } catch (NumberFormatException e) {
                // Se o valor não puder ser convertido, ignora a linha
                return;
            }

            // lógica simples para identificar padrões suspeitos
            if (transactionAmount > 1000) {
                pattern.set("HighValue_" + category + "_" + location);
            } else if (transactionAmount < 1) {
                pattern.set("LowValue_" + category + "_" + location);
            } else {
                pattern.set("Normal_" + category + "_" + location);
            }

            context.write(pattern, one);  // Emitindo o padrão detectado como chave

        } catch (Exception e) {
            // Captura todas as exceções para evitar que o job falhe devido a uma linha mal formatada
            e.printStackTrace();
        } finally {
            reader.close();
        }
    }
}
