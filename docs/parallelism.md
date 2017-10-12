# Paralellism

`--parallelism` is a flag available on `rolling-update`.  This flag is better thought of as "chunking" a deployment, instead
of true paralellism.

Lets say we have hosts **A**, **B**, and **C**, each in the running state:

| A | B | C |
|---|---|---|
| R | R | R |

We issue a `rolling-update` with `--parallelism` set to 2. The first thing that happens is that **A** and **B** undeploy their currently running job, entering the undeployed state. Note that undeploying is an asynchronous operation for the Helios master as well as the Helios agents on hosts **A** and **B**. The host **C** is unaffected:

| A | B | C |
|---|---|---|
| U | U | R |

The next thing that happens is that **A** and **B** are put into the deploy state [in that order]. Again, this operation is asynchronous. Host **C** is still  unaffected:

| A | B | C |
|---|---|---|
| D | D | R |

Next, **A** and **B** go to the wait state. The rolling update will block until *A* converges to the running state. Host **C** is still unaffected:

| A | B | C |
|---|---|---|
| W | W | R |

Now that **A** is running again, the rolling update will continue with waiting for **B** to complete deploying successfully. Since **B** has been deploying asynchrously while the rolling update waited for **A** to finish, the time to wait is typically very short. This is the mechanism that makes `--parallelism` decrease the time it takes for a deployment to finish.

| A | B | C |
|---|---|---|
| R | W | R |

Host **B** has entered the running state, which means this "chunk" has finished deploying.

| A | B | C |
|---|---|---|
| R | R | R |

Host **C** is is the only machine in its "chunk", so it deploys in the same way a single job deployment would:

| A | B | C |
|---|---|---|
| R | R | U |

 
| A | B | C |
|---|---|---|
| R | R | D |

| A | B | C |
|---|---|---|
| R | R | W |

| A | B | C |
|---|---|---|
| R | R | R |