using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.Networking;

public class Test : MonoBehaviour
{

    public IEnumerator coroutine;
    // Start is called before the first frame update
    void Start()
    {
        coroutine = UploadSomehtin();
        StartCoroutine(coroutine);
    }

    // Update is called once per frame
    void Update()
    {
        
    }

    public IEnumerator UploadSomehtin()
    {
        KafkaObj obj = new KafkaObj();
        obj.name = "Jose";
        obj.level = 2;
        string json = JsonUtility.ToJson(obj);
        Debug.Log(json);
        UnityWebRequest www = UnityWebRequest.Post("", "");
        yield return null;
    }
}

class KafkaObj
{
    public string name;
    public int level;
}
