using com.flyingcrow.kafka;
using UnityEngine;

public class PlayerMovement : MonoBehaviour
{
    [SerializeField]
    [Range(1,10)]
    private float movementSpeed;

    private Vector3 prevPosition;
    
    // Update is called once per frame
    void FixedUpdate()
    {
        MoveCharacter();
        SendPositionInformation();
    }

    private void MoveCharacter()
    {
        Vector3 movement = Vector3.zero;

        float x = Input.GetAxis("Horizontal");
        float y = Input.GetAxis("Vertical");

        movement += Vector3.right * x;
        movement += Vector3.forward * y;

        if (movement.magnitude > 1) movement.Normalize();

        movement *= movementSpeed * Time.deltaTime;

        this.GetComponent<CharacterController>().Move(movement);
    }

    private void SendPositionInformation()
    {
        if (!transform.position.Equals(prevPosition))
        {
            this.GetComponent<KafkaProducer>().SendMovement(transform.position);
            prevPosition = transform.position;
        }
    }
}
