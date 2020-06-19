/*
 * Copyright (c) 2020, NVIDIA CORPORATION.  All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"fmt"

	"github.com/NVIDIA/gpu-monitoring-tools/bindings/go/nvml"
	pluginapi "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"
)

const (
	MigStrategyNone                       = "none"
	MigStrategySingle                     = "single"
	MigStrategyMixed                      = "mixed"
	MigStrategyMixedMemoryQualified       = "mixed-memory-qualified"
	MigStrategyMixedFractionallyQualified = "mixed-fractionally-qualified"
)

type MigStrategyResourceSet map[string]struct{}

type MigStrategy interface {
	GetPlugins() []*NvidiaDevicePlugin
	MatchesResource(mig *nvml.Device, resource string) bool
}

func NewMigStrategy(strategy string) (MigStrategy, error) {
	switch strategy {
	case MigStrategyNone:
		return &migStrategyNone{}, nil
	case MigStrategySingle:
		return &migStrategySingle{}, nil
	case MigStrategyMixed:
		return &migStrategyMixed{}, nil
	case MigStrategyMixedMemoryQualified:
		return &migStrategyMixedMemoryQualified{}, nil
	case MigStrategyMixedFractionallyQualified:
		return &migStrategyMixedFractionallyQualified{}, nil
	}
	return nil, fmt.Errorf("Unknown strategy: %v", strategy)
}

type migStrategyNone struct{}
type migStrategySingle struct{}
type migStrategyMixed struct{}
type migStrategyMixedMemoryQualified struct{}
type migStrategyMixedFractionallyQualified struct{}

// getAllMigDevices() across all full GPUs
func getAllMigDevices() []*nvml.Device {
	n, err := nvml.GetDeviceCount()
	check(err)

	var migs []*nvml.Device
	for i := uint(0); i < n; i++ {
		d, err := nvml.NewDeviceLite(i)
		check(err)

		migEnabled, err := d.IsMigEnabled()
		check(err)

		if !migEnabled {
			continue
		}

		devs, err := d.GetMigDevices()
		check(err)

		migs = append(migs, devs...)
	}

	return migs
}

// migStrategyNone
func (s *migStrategyNone) GetPlugins() []*NvidiaDevicePlugin {
	return []*NvidiaDevicePlugin{
		NewNvidiaDevicePlugin(
			"nvidia.com/gpu",
			NewGpuDeviceManager(false), // Enumerate device even if MIG enabled
			"NVIDIA_VISIBLE_DEVICES",
			pluginapi.DevicePluginPath+"nvidia-gpu.sock"),
	}
}

func (s *migStrategyNone) MatchesResource(mig *nvml.Device, resource string) bool {
	panic("Should never be called")
	return false
}

// migStrategySingle
func (s *migStrategySingle) GetPlugins() []*NvidiaDevicePlugin {
	resources := make(MigStrategyResourceSet)
	for _, mig := range getAllMigDevices() {
		r := s.getResourceName(mig)
		resources[r] = struct{}{}
	}

	if len(resources) != 1 {
		panic("More than one MIG device type present on node")
	}

	return []*NvidiaDevicePlugin{
		NewNvidiaDevicePlugin(
			"nvidia.com/gpu",
			NewMigDeviceManager(s, "gpu"),
			"NVIDIA_VISIBLE_DEVICES",
			pluginapi.DevicePluginPath+"nvidia-gpu.sock"),
	}
}

func (s *migStrategySingle) getResourceName(mig *nvml.Device) string {
	attr, err := mig.GetAttributes()
	check(err)

	g := attr.GpuInstanceSliceCount
	c := attr.ComputeInstanceSliceCount
	gb := ((attr.MemorySizeMB + 1000 - 1) / 1000)
	r := fmt.Sprintf("mig-%dc.%dg.%dgb", c, g, gb)

	return r
}

func (s *migStrategySingle) MatchesResource(mig *nvml.Device, resource string) bool {
	return true
}

// migStrategyMixedGetPlugins is shared by each of the mixed strategies below
func migStrategyMixedGetPlugins(s MigStrategy, resources MigStrategyResourceSet) []*NvidiaDevicePlugin {
	plugins := []*NvidiaDevicePlugin{
		NewNvidiaDevicePlugin(
			"nvidia.com/gpu",
			NewGpuDeviceManager(true),
			"NVIDIA_VISIBLE_DEVICES",
			pluginapi.DevicePluginPath+"nvidia-gpu.sock"),
	}

	for resource := range resources {
		plugin := NewNvidiaDevicePlugin(
			"nvidia.com/"+resource,
			NewMigDeviceManager(s, resource),
			"NVIDIA_VISIBLE_DEVICES",
			pluginapi.DevicePluginPath+"nvidia-"+resource+".sock")
		plugins = append(plugins, plugin)
	}

	return plugins
}

// migStrategyMixed
func (s *migStrategyMixed) GetPlugins() []*NvidiaDevicePlugin {
	resources := make(MigStrategyResourceSet)
	for _, mig := range getAllMigDevices() {
		r := s.getResourceName(mig)
		resources[r] = struct{}{}
	}
	return migStrategyMixedGetPlugins(s, resources)
}

func (s *migStrategyMixed) getResourceName(mig *nvml.Device) string {
	attr, err := mig.GetAttributes()
	check(err)

	g := attr.GpuInstanceSliceCount
	gb := ((attr.MemorySizeMB + 1000 - 1) / 1000)
	r := fmt.Sprintf("mig-%dg.%dgb", g, gb)

	return r
}

func (s *migStrategyMixed) MatchesResource(mig *nvml.Device, resource string) bool {
	return s.getResourceName(mig) == resource
}

// migStrategyMixedMemoryQualified
func (s *migStrategyMixedMemoryQualified) GetPlugins() []*NvidiaDevicePlugin {
	resources := make(MigStrategyResourceSet)
	for _, mig := range getAllMigDevices() {
		r := s.getResourceName(mig)
		resources[r] = struct{}{}
	}
	return migStrategyMixedGetPlugins(s, resources)
}

func (s *migStrategyMixedMemoryQualified) getResourceName(mig *nvml.Device) string {
	attr, err := mig.GetAttributes()
	check(err)

	gb := ((attr.MemorySizeMB + 1000 - 1) / 1000)
	r := fmt.Sprintf("mig-%dgb", gb)

	return r
}

func (s *migStrategyMixedMemoryQualified) MatchesResource(mig *nvml.Device, resource string) bool {
	return s.getResourceName(mig) == resource
}

// migStrategyMixedFractionallyQualified
func (s *migStrategyMixedFractionallyQualified) GetPlugins() []*NvidiaDevicePlugin {
	resources := make(MigStrategyResourceSet)
	for _, mig := range getAllMigDevices() {
		r := s.getResourceName(mig)
		resources[r] = struct{}{}
	}
	return migStrategyMixedGetPlugins(s, resources)
}

func (s *migStrategyMixedFractionallyQualified) getResourceName(mig *nvml.Device) string {
	parent, err := mig.GetMigParentDeviceLite()
	check(err)

	maxMigs, err := parent.GetMaxMigDeviceCount()
	check(err)

	attr, err := mig.GetAttributes()
	check(err)

	// The following algorithm is customized knowing we have 7 slices on
	// Ampere. It interprets a MigHalf as 3, MigQuarter as 2 and MigEighth
	// as 1. On future GPUs we should have 8 maximum MIG devices, not 7, so
	// this will need to change.
	//
	// TODO: generalize this for future architectures 8 or more slices.
	if int(attr.GpuInstanceSliceCount) == ((maxMigs+1)/2)-1 {
		return "mig-half"
	}
	if int(attr.GpuInstanceSliceCount) == ((maxMigs + 1) / 4) {
		return "mig-quarter"
	}
	if int(attr.GpuInstanceSliceCount) == ((maxMigs + 1) / 8) {
		return "mig-eighth"
	}

	panic("Unsupported MIG instance size")
}

func (s *migStrategyMixedFractionallyQualified) MatchesResource(mig *nvml.Device, resource string) bool {
	return s.getResourceName(mig) == resource
}
