using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class PlayerController : MonoBehaviour
{

    [SerializeField]
    private PlayerMovement playerMovement;

    [SerializeField]
    private KafkaPlayer selectedPlayer;

    // Start is called before the first frame update
    void Start()
    {
        playerMovement.setCurrentPlayer(selectedPlayer);
    }

    // Update is called once per frame
    void Update()
    {
        
    }
}
