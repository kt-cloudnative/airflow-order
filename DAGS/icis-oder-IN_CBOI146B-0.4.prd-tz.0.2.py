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
                , WORKFLOW_NAME='IN_CBOI146B',WORKFLOW_ID='1e68693f0bf447f2a02e0451e1034956', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-IN_CBOI146B-0.4.prd-tz.0.2'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 1, 10, 10, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('1e68693f0bf447f2a02e0451e1034956')

    cboi146bJob_vol = []
    cboi146bJob_volMnt = []
    cboi146bJob_vol.append(getVolume('shared-volume','shared-volume'))
    cboi146bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cboi146bJob_env = [getICISConfigMap('icis-oder-intelnet-batch-configmap'), getICISConfigMap('icis-oder-intelnet-batch-configmap2'), getICISSecret('icis-oder-intelnet-batch-secret')]
    cboi146bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    cboi146bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cboi146bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'e6dd3bf8aa8746ffb92de1bfe81690b0',
        'volumes': cboi146bJob_vol,
        'volume_mounts': cboi146bJob_volMnt,
        'env_from':cboi146bJob_env,
        'task_id':'cboi146bJob',
        'image':'/icis/icis-oder-intelnet-batch:0.7.1.1',
        'arguments':["--job.name=cboi146bJob", "endTranDate=20240612", "date=${YYYYMMDDHHMISS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('1e68693f0bf447f2a02e0451e1034956')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cboi146bJob,
        Complete
    ]) 

    # authCheck >> cboi146bJob >> Complete
    workflow








