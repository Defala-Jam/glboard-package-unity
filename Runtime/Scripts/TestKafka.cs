using UnityEngine;
using System.Text;
using System.Threading.Tasks;

public class TestKafka : MonoBehaviour
{
    private KafkaProducer producer;

    private async void Start()
    {
        Debug.Log("[TestKafka] Start iniciado");

        producer = new KafkaProducer("localhost:9094"); 
        Debug.Log("[TestKafka] Producer criado");

        try
        {
            var topic = "glb.upm.events";
            var key = "unity-test";
            var message = "Mensagem de teste do Unity!";
            Debug.Log("[TestKafka] Enviando...");

            var offset = await producer.SendAsync(topic, key, Encoding.UTF8.GetBytes(message));

            if (offset != null)
                Debug.Log($"[TestKafka] Mensagem enviada para {topic}: {offset}");
            else
                Debug.LogError("[TestKafka] Falha ao enviar mensagem!");
        }
        catch (System.Exception ex)
        {
            Debug.LogError($"[TestKafka] Erro: {ex}");
        }
    }

    private void OnApplicationQuit()
    {
        producer?.Flush();
        producer?.Dispose();
    }
}
