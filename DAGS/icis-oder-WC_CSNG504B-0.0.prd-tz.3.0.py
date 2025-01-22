from datetime import datetime, timedelta
from kubernetes.client import models as k8s
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.subdag import SubDagOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.helpers import chain, cross_downstream
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
import pendulum
local_tz = pendulum.timezone("Asia/Seoul")
import sys
sys.path.append('/opt/bitnami/airflow/dags/git_sa-common')

from icis_common import *
COMMON = ICISCmmn(DOMAIN='oder',ENV='prd-tz', NAMESPACE='t-order'
                , WORKFLOW_NAME='WC_CSNG504B',WORKFLOW_ID='54ec39a6292940eeb62fab1644f15984', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-WC_CSNG504B-0.0.prd-tz.3.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2023, 11, 3, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs':1
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('54ec39a6292940eeb62fab1644f15984')

    csng504bJob_0131_vol = []
    csng504bJob_0131_volMnt = []
    csng504bJob_0131_vol.append(getVolume('shared-volume','shared-volume'))
    csng504bJob_0131_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csng504bJob_0131_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csng504bJob_0131_env.extend([getICISConfigMap('icis-oder-wrlincomn-batch-mng-configmap'), getICISSecret('icis-oder-wrlincomn-batch-mng-secret'), getICISConfigMap('icis-oder-wrlincomn-batch-configmap'), getICISSecret('icis-oder-wrlincomn-batch-secret')])
    csng504bJob_0131_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng504bJob_0131 = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'c2cbbdbfe06b44bba75168ee6ddf58b9',
        'volumes': csng504bJob_0131_vol,
        'volume_mounts': csng504bJob_0131_volMnt,
        'env_from':csng504bJob_0131_env,
        'task_id':'csng504bJob_0131',
        'image':'/icis/icis-oder-wrlincomn-batch:0.7.1.8',
        'arguments':["--job.name=csng504bJob", "date="+str(datetime.now()), "inputDate=${YYYYMM}01", "workMode=0131"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    csng504bJob_0502_vol = []
    csng504bJob_0502_volMnt = []
    csng504bJob_0502_vol.append(getVolume('shared-volume','shared-volume'))
    csng504bJob_0502_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csng504bJob_0502_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csng504bJob_0502_env.extend([getICISConfigMap('icis-oder-wrlincomn-batch-mng-configmap'), getICISSecret('icis-oder-wrlincomn-batch-mng-secret'), getICISConfigMap('icis-oder-wrlincomn-batch-configmap'), getICISSecret('icis-oder-wrlincomn-batch-secret')])
    csng504bJob_0502_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng504bJob_0502 = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'b842a71b37ef4cd18ae5b4f5b7092206',
        'volumes': csng504bJob_0502_vol,
        'volume_mounts': csng504bJob_0502_volMnt,
        'env_from':csng504bJob_0502_env,
        'task_id':'csng504bJob_0502',
        'image':'/icis/icis-oder-wrlincomn-batch:0.7.1.8',
        'arguments':["--job.name=csng504bJob", "date="+str(datetime.now()), "inputDate=${YYYYMM}01", "workMode=0502"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    csng504bJob_0060_vol = []
    csng504bJob_0060_volMnt = []
    csng504bJob_0060_vol.append(getVolume('shared-volume','shared-volume'))
    csng504bJob_0060_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csng504bJob_0060_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csng504bJob_0060_env.extend([getICISConfigMap('icis-oder-wrlincomn-batch-mng-configmap'), getICISSecret('icis-oder-wrlincomn-batch-mng-secret'), getICISConfigMap('icis-oder-wrlincomn-batch-configmap'), getICISSecret('icis-oder-wrlincomn-batch-secret')])
    csng504bJob_0060_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng504bJob_0060 = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'fbbbbac7cdca4c7e843c25a17ef57130',
        'volumes': csng504bJob_0060_vol,
        'volume_mounts': csng504bJob_0060_volMnt,
        'env_from':csng504bJob_0060_env,
        'task_id':'csng504bJob_0060',
        'image':'/icis/icis-oder-wrlincomn-batch:0.7.1.8',
        'arguments':["--job.name=csng504bJob", "date="+str(datetime.now()), "inputDate=${YYYYMM}01", "workMode=0060"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    csng504bJob_0303_vol = []
    csng504bJob_0303_volMnt = []
    csng504bJob_0303_vol.append(getVolume('shared-volume','shared-volume'))
    csng504bJob_0303_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csng504bJob_0303_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csng504bJob_0303_env.extend([getICISConfigMap('icis-oder-wrlincomn-batch-mng-configmap'), getICISSecret('icis-oder-wrlincomn-batch-mng-secret'), getICISConfigMap('icis-oder-wrlincomn-batch-configmap'), getICISSecret('icis-oder-wrlincomn-batch-secret')])
    csng504bJob_0303_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng504bJob_0303 = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '7d4c87dc0e86454089745873eff2d7fe',
        'volumes': csng504bJob_0303_vol,
        'volume_mounts': csng504bJob_0303_volMnt,
        'env_from':csng504bJob_0303_env,
        'task_id':'csng504bJob_0303',
        'image':'/icis/icis-oder-wrlincomn-batch:0.7.1.8',
        'arguments':["--job.name=csng504bJob", "date="+str(datetime.now()), "inputDate=${YYYYMM}01", "workMode=0303"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    csng504bJob_0080_vol = []
    csng504bJob_0080_volMnt = []
    csng504bJob_0080_vol.append(getVolume('shared-volume','shared-volume'))
    csng504bJob_0080_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csng504bJob_0080_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csng504bJob_0080_env.extend([getICISConfigMap('icis-oder-wrlincomn-batch-mng-configmap'), getICISSecret('icis-oder-wrlincomn-batch-mng-secret'), getICISConfigMap('icis-oder-wrlincomn-batch-configmap'), getICISSecret('icis-oder-wrlincomn-batch-secret')])
    csng504bJob_0080_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng504bJob_0080 = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'f1500af40de64d17830c7e8409a00cd7',
        'volumes': csng504bJob_0080_vol,
        'volume_mounts': csng504bJob_0080_volMnt,
        'env_from':csng504bJob_0080_env,
        'task_id':'csng504bJob_0080',
        'image':'/icis/icis-oder-wrlincomn-batch:0.7.1.8',
        'arguments':["--job.name=csng504bJob", "date="+str(datetime.now()), "inputDate=${YYYYMM}01", "workMode=0080"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('54ec39a6292940eeb62fab1644f15984')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csng504bJob_0502,
        csng504bJob_0060,
        csng504bJob_0303,
        csng504bJob_0080,
        csng504bJob_0131,
        Complete
    ]) 

    # authCheck >> csng504bJob_0502 >> csng504bJob_0060 >> csng504bJob_0303>> csng504bJob_0080 >> csng504bJob_0131 >> Complete
    workflow








