# Phase 1: CRD Core Concepts Deep Dive 🚀

*Welcome to your journey into the heart of Kubernetes extensibility! Let's unpack these concepts like we're exploring a fascinating new city - each concept builds on the last, creating a complete picture.*

---

## 🎯 What CRDs Actually Are (And Why They're Mind-Blowing)

**Think of Kubernetes as LEGO system**. Out of the box, you get standard blocks: Pods, Services, Deployments. But what if you want to build a spaceship and there's no spaceship block? 

**That's where CRDs come in** - they're your custom LEGO block factory! 

### The "Aha!" Moment
```yaml
# Before CRDs: You're stuck with what Kubernetes gives you
apiVersion: v1
kind: Pod  # ← You can only use built-in types

# After CRDs: You can create your own!
apiVersion: mycompany.io/v1alpha1
kind: AppConfig  # ← YOU invented this type!
```

**Why this matters**: The Kubernetes API lets you query and manipulate the state of objects in Kubernetes - and CRDs let YOU add new objects to that API. You're literally extending Kubernetes itself!

### Real-World Analogy
Imagine Kubernetes as a restaurant kitchen:
- **Built-in resources** = Standard appliances (oven, stove, fridge)
- **CRDs** = Your custom invention (maybe a pizza-pasta hybrid maker!)
- **Controllers** = The chef who knows how to use your invention

---

## 🔄 Built-in vs Custom Resources: The Great Divide

### Built-in Resources (The Kubernetes Classics)
```bash
# These are baked into Kubernetes core
kubectl get pods      # Always available
kubectl get services  # Always available
kubectl get deployments # Always available
```

**Key traits:**
- Hardcoded in Kubernetes source code
- Can't modify their behavior
- Have mature, stable APIs
- Come with built-in controllers

### Custom Resources (Your Creative Canvas)
```bash
# These are YOUR inventions
kubectl get appconfigs    # ← Only exists if YOU created it
kubectl get databases     # ← Only exists if someone defined it
kubectl get pizzaorders   # ← Yes, you could make this! 🍕
```

**Key traits:**
- Defined by YOU using CRD manifests
- Behavior defined by YOUR controllers
- Start as experiments, mature over time
- You want top-level support from kubectl; for example, kubectl get my-object object-name

### The Power Dynamic
```yaml
# This CRD definition tells Kubernetes:
# "Hey! Treat 'AppConfig' like a first-class citizen"
apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: appconfigs.config.mycompany.io
spec:
  group: config.mycompany.io
  versions:
  - name: v1alpha1
    served: true
    storage: true
    schema: # ← This is where the magic happens!
```

---

## 🌐 API Server Interaction Patterns: The Kubernetes Traffic Control

### How Kubernetes API Works (The Big Picture)
```
You → kubectl → API Server → etcd
 ↑                ↓
 ← JSON Response ← Controller
```

**The API Server is like a super-smart receptionist:**
1. Receives your requests
2. Validates them (using schemas!)
3. Stores them in etcd
4. Notifies controllers about changes
5. Returns responses

### CRD Integration Magic
When you create a CRD, something amazing happens:

```bash
# Before CRD exists
kubectl get appconfigs
# Error: the server doesn't have a resource type "appconfigs"

# After CRD is applied
kubectl apply -f appconfig-crd.yaml
kubectl get appconfigs
# No resources found in default namespace. ← IT WORKS!
```

**What just happened?** The API server dynamically learned about your new resource type! It's like teaching someone a new language word, and they immediately start using it fluently.

### The Request Flow
```
1. kubectl get appconfigs
2. API Server checks: "Do I know about appconfigs?"
3. API Server finds your CRD: "Yes! It's defined as config.mycompany.io/v1alpha1"
4. API Server queries etcd for appconfig objects
5. Returns results in the same format as built-in resources
```

---

## ♻️ Resource Lifecycle Management: Birth, Life, Death

### The Journey of a Custom Resource

**Phase 1: Birth (Creation)**
```yaml
# You create an AppConfig
apiVersion: config.mycompany.io/v1alpha1
kind: AppConfig
metadata:
  name: my-app-config
spec:
  appName: "awesome-app"
  environment: "production"
```

**Phase 2: Life (Updates & Reconciliation)**
- Your controller watches for changes
- Users can update the resource
- Controller ensures actual state matches desired state
- Status gets updated to reflect current conditions

**Phase 3: Death (Deletion)**
- User runs `kubectl delete appconfig my-app-config`
- Controller gets deletion event
- Controller cleans up related resources (finalizers!)
- Resource gets removed from etcd

### Owner References: The Family Tree
```yaml
# Your AppConfig creates a ConfigMap
apiVersion: v1
kind: ConfigMap
metadata:
  name: my-app-config-cm
  ownerReferences:  # ← This creates parent-child relationship
  - apiVersion: config.mycompany.io/v1alpha1
    kind: AppConfig
    name: my-app-config
    uid: 12345-67890
```

**Result**: Delete the AppConfig → ConfigMap gets automatically deleted! It's like Kubernetes garbage collection.

---

## 🛡️ OpenAPI v3 Schema Validation: Your Data Bodyguard

### Why Validation Matters
Without schemas, users could create nonsense:
```yaml
# Without validation, this garbage would be accepted!
apiVersion: config.mycompany.io/v1alpha1
kind: AppConfig
spec:
  appName: 42  # ← Should be string!
  environment: "jupiter"  # ← Not a valid environment!
  crazyField: "this shouldn't exist"
```

### Schema Power in Action
From Kubernetes v1.15, CustomResources(CR) is able to adopt a schema of Open API V3 validation by specifying it in a definition file. The schema is used to validate the JSON data during creation and updates so that it can prevent from invalid data, moreover, from malicious attacks.

```yaml
# In your CRD definition
spec:
  versions:
  - name: v1alpha1
    schema:
      openAPIV3Schema:
        type: object
        properties:
          spec:
            type: object
            properties:
              appName:
                type: string
                minLength: 1  # ← No empty names!
                maxLength: 50 # ← Reasonable limits
              environment:
                type: string
                enum: ["dev", "staging", "prod"]  # ← Only valid envs!
            required: ["appName", "environment"]  # ← Must provide these!
```

### Validation in Real-Time
```bash
# Try to create invalid resource
kubectl apply -f bad-appconfig.yaml
# error validating data: ValidationError: invalid value for environment
```

**It's like having a bouncer for your API!** Only well-formed data gets through.

---

## 📊 Resource Versioning: The Evolution Story

### The Version Journey
For the example in Specify multiple versions, the version sort order is v1, followed by v1beta1. As the Kubernetes API evolves, APIs are periodically reorganized or upgraded.

Think of versions like software releases:

**v1alpha1** = "Hey, I'm experimenting with this idea"
- Breaking changes expected
- Use at your own risk
- Perfect for learning and prototyping

**v1beta1** = "This is getting serious, but might still change"
- Fewer breaking changes
- More stable, but not guaranteed
- Good for testing in non-critical environments

**v1** = "This is battle-tested and stable"
- No breaking changes (backwards compatible)
- Production ready
- Long-term support

### Version Evolution Example
```yaml
# Year 1: Your first attempt
apiVersion: config.mycompany.io/v1alpha1
kind: AppConfig
spec:
  name: "my-app"  # Simple design

# Year 2: You learned more
apiVersion: config.mycompany.io/v1alpha2
kind: AppConfig
spec:
  appName: "my-app"      # ← Renamed field (breaking change!)
  environment: "prod"    # ← New required field
  
# Year 3: Ready for production
apiVersion: config.mycompany.io/v1beta1
kind: AppConfig
spec:
  appName: "my-app"
  environment: "prod"
  configData: {}         # ← Added more features

# Year 4: Stable and mature
apiVersion: config.mycompany.io/v1
kind: AppConfig  # Same structure as v1beta1, just marked stable
```

### Multiple Version Support
Your CRD can serve multiple versions simultaneously:
```yaml
spec:
  versions:
  - name: v1alpha1
    served: true
    storage: false  # ← Old version, don't store new objects here
  - name: v1beta1
    served: true
    storage: true   # ← New objects stored in this version
```

---

## 🎭 Subresources: The Hidden Superpowers

### Status Subresource: The Progress Reporter
```yaml
# Your main resource
apiVersion: config.mycompany.io/v1alpha1
kind: AppConfig
spec:
  appName: "awesome-app"
status:  # ← This is a SUBRESOURCE!
  configMapName: "awesome-app-cm"
  lastUpdated: "2024-01-15T10:30:00Z"
  conditions:
  - type: "Ready"
    status: "True"
    reason: "ConfigMapCreated"
```

**Why separate status?**
- Users can't directly modify status (controller-only!)
- Different RBAC permissions
- Cleaner API design

### Scale Subresource: The Growth Manager
```yaml
# Enable scaling for your resource
spec:
  subresources:
    scale:
      specReplicasPath: .spec.replicas
      statusReplicasPath: .status.replicas
```

**Result**: Your custom resource can be scaled like Deployments!
```bash
kubectl scale appconfig my-app --replicas=5
```

---

## 🌍 Resource Scope: Neighborhood vs City-Wide

### Namespaced Resources (Neighborhood Level)
```yaml
# Lives in a specific namespace
apiVersion: config.mycompany.io/v1alpha1
kind: AppConfig
metadata:
  name: my-config
  namespace: production  # ← Isolated to this namespace
```

**Perfect for**:
- Application-specific configs
- Team-isolated resources
- Multi-tenant scenarios

### Cluster-Scoped Resources (City-Wide)
```yaml
# Available across entire cluster
apiVersion: config.mycompany.io/v1alpha1
kind: ClusterAppConfig  # Note: different kind name
metadata:
  name: global-config  # ← No namespace field!
```

**Perfect for**:
- Global configurations
- Infrastructure-level resources
- Admin-only resources

### Scope Definition in CRD
```yaml
spec:
  scope: Namespaced  # or "Cluster"
  group: config.mycompany.io
  names:
    plural: appconfigs
    singular: appconfig
    kind: AppConfig
```

---

## 🎯 Hands-On Mission: Your First CRD Adventure!

### Mission 1: Reconnaissance
```bash
# Discover existing CRDs in your cluster
kubectl get crd

# Pick an interesting one and examine it
kubectl describe crd <name-from-above>

# See what custom resources exist
kubectl get <plural-name> --all-namespaces
```

### Mission 2: Create Your First CRD
```yaml
# Save as: simple-appconfig-crd.yaml
apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: appconfigs.config.example.com
spec:
  group: config.example.com
  versions:
  - name: v1alpha1
    served: true
    storage: true
    schema:
      openAPIV3Schema:
        type: object
        properties:
          spec:
            type: object
            properties:
              appName:
                type: string
              environment:
                type: string
                enum: ["dev", "staging", "prod"]
            required: ["appName"]
  scope: Namespaced
  names:
    plural: appconfigs
    singular: appconfig
    kind: AppConfig
```

### Mission 3: Test Your Creation
```bash
# Deploy your CRD
kubectl apply -f simple-appconfig-crd.yaml

# Verify it exists
kubectl get crd appconfigs.config.example.com

# Create a custom resource
cat << EOF | kubectl apply -f -
apiVersion: config.example.com/v1alpha1
kind: AppConfig
metadata:
  name: my-first-config
spec:
  appName: "hello-world"
  environment: "dev"
EOF

# List your custom resources
kubectl get appconfigs

# Try creating an invalid one (should fail!)
cat << EOF | kubectl apply -f -
apiVersion: config.example.com/v1alpha1
kind: AppConfig
metadata:
  name: invalid-config
spec:
  appName: "test"
  environment: "mars"  # ← Not in enum!
EOF
```

---

## 🎉 Congratulations! You've Unlocked CRD Mastery Level 1!

You now understand:
- ✅ CRDs are your custom LEGO blocks for Kubernetes
- ✅ They integrate seamlessly with the Kubernetes API
- ✅ Schemas protect your API from bad data
- ✅ Versioning helps your API evolve gracefully
- ✅ Subresources add powerful capabilities
- ✅ Scope determines resource visibility

**Next up**: In Phase 2, we'll build controllers to make your custom resources actually DO something! 🚀

*Remember: Every expert was once a beginner. You're now armed with the knowledge to extend Kubernetes itself - that's pretty amazing!*