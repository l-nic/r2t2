#ifndef PBRT_ACCELERATORS_CLOUD_BVH_H
#define PBRT_ACCELERATORS_CLOUD_BVH_H

#include <map>
#include <memory>
#include <stack>
#include <vector>

#include "cloud/raystate.h"
#include "core/pbrt.h"
#include "core/primitive.h"
#include "core/transform.h"
#include "messages/serialization.h"
#include "shapes/triangle.h"
#include "util/optional.h"

namespace pbrt {

struct TreeletNode;

class CloudBVH : public Aggregate {
  public:
    struct TreeletInfo {
        std::set<uint32_t> children{};
        std::set<uint32_t> instances{};
        std::vector<Bounds3f> treeletNodeBounds;
        Bounds3f bounds{};
    };

    CloudBVH(const uint32_t bvh_root = 0);
    ~CloudBVH() {}

    CloudBVH(const CloudBVH &) = delete;
    CloudBVH &operator=(const CloudBVH &) = delete;

    Bounds3f WorldBound() const;
    bool Intersect(const Ray &ray, SurfaceInteraction *isect) const;
    bool IntersectP(const Ray &ray) const;

    void Trace(RayState &rayState);
    bool Intersect(RayState &rayState, SurfaceInteraction *isect) const;

    const TreeletInfo &GetInfo(const uint32_t treelet_id) {
        loadTreelet(treelet_id);
        treelet_info_.at(treelet_id).bounds = WorldBound();
        treelet_info_.at(treelet_id).treeletNodeBounds =
            getTreeletNodeBounds(treelet_id, 4);
        // treelet_info_.at(treelet_id).worldNode =
        // treelets_.at(treelet_id).nodes[0];
        return treelet_info_.at(treelet_id);
    }

    struct TreeletNode {
        Bounds3f bounds;
        uint8_t axis;

        union {
            struct {
                uint16_t child_treelet[2] = {0};
                uint32_t child_node[2] = {0};
            };
            struct {
                uint32_t leaf_tag;
                uint32_t primitive_offset;
                uint32_t primitive_count;
            };
        };

        TreeletNode(const Bounds3f &bounds, const uint8_t axis)
            : bounds(bounds), axis(axis) {}

        bool is_leaf() const { return leaf_tag == ~0; }
    };

  private:
    enum Child { LEFT = 0, RIGHT = 1 };

    struct Treelet {
        std::vector<TreeletNode> nodes{};
        std::vector<std::unique_ptr<Primitive>> primitives{};
    };

    class IncludedInstance : public Aggregate {
      public:
        IncludedInstance(const Treelet *treelet, int nodeIdx)
            : treelet_(treelet), nodeIdx_(nodeIdx)
        {}

        Bounds3f WorldBound() const;
        bool Intersect(const Ray &ray, SurfaceInteraction *isect) const;
        bool IntersectP(const Ray &ray) const;

      private:
        const Treelet *treelet_;
        int nodeIdx_;
    };

    const std::string bvh_path_;
    const uint32_t bvh_root_;

    mutable std::map<uint32_t, Treelet> treelets_;
    mutable std::map<uint64_t, std::shared_ptr<Primitive>> bvh_instances_;
    mutable std::vector<std::unique_ptr<Transform>> transforms_;
    mutable std::map<uint32_t, std::shared_ptr<TriangleMesh>> triangle_meshes_;
    mutable std::map<uint32_t, uint32_t> triangle_mesh_material_ids_;
    mutable std::map<uint32_t, std::shared_ptr<Material>> materials_;

    mutable std::shared_ptr<Material> default_material;

    mutable std::map<uint32_t, TreeletInfo> treelet_info_;

    void loadTreelet(const uint32_t root_id) const;
    void clear() const;

    // returns array of Bounds3f with structure of Treelet's internal BVH nodes
    std::vector<Bounds3f> getTreeletNodeBounds(
        const uint32_t treelet_id, const int recursionLimit = 4) const;

    void recurseBVHNodes(const int depth, const int recursionLimit,
                         const int idx, const Treelet &currTreelet,
                         const TreeletNode &currNode,
                         std::vector<Bounds3f> &treeletBounds) const;

    Transform identity_transform_;
};

std::shared_ptr<CloudBVH> CreateCloudBVH(const ParamSet &ps);

}  // namespace pbrt

#endif /* PBRT_ACCELERATORS_CLOUD_BVH_H */
