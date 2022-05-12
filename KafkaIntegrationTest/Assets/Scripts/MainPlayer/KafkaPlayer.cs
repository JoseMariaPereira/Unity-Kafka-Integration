using com.flyingcrow.kafka;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class KafkaPlayer : MonoBehaviour
{
    [SerializeField]
    private string playerName;

    public string getPlayerName()
    {
        return playerName;
    }

    private void Start()
    {
        transform.GetComponent<KafkaConsumer>().StartKafkaConsumer(playerName);
    }
}
