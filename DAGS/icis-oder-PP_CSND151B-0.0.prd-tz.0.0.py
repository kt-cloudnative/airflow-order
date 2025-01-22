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
                , WORKFLOW_NAME='PP_CSND151B',WORKFLOW_ID='1376eb8023544f43a06b38e987e60a79', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSND151B-0.0.prd-tz.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 1, 12, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('1376eb8023544f43a06b38e987e60a79')

    csnd151bJob_vol = []
    csnd151bJob_volMnt = []
    csnd151bJob_vol.append(getVolume('shared-volume','shared-volume'))
    csnd151bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csnd151bJob_env = [getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISConfigMap('icis-oder-ppon-batch-configmap2'), getICISSecret('icis-oder-ppon-batch-secret')]
    csnd151bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    csnd151bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csnd151bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '50d58d23cb68498d8ed26ebccea198b1',
        'volumes': csnd151bJob_vol,
        'volume_mounts': csnd151bJob_volMnt,
        'env_from':csnd151bJob_env,
        'task_id':'csnd151bJob',
        'image':'/icis/icis-oder-ppon-batch:0.4.1.45',
        'arguments':["--job.name=csnd151bJob","endTranDate=20240927", "requestDate="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('1376eb8023544f43a06b38e987e60a79')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csnd151bJob,
        Complete
    ]) 

    # authCheck >> csnd151bJob >> Complete
    workflow








