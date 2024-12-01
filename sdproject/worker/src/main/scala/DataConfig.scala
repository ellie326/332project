package worker

object DataConfig{
    val length_key: Int = 10
    val length_value: Int = 90
    val length_data: Int = length_key + length_value

    val num_sample: Int = 1000
    val blockConfig: Int = 20*1000 
}
